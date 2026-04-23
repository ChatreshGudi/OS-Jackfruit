/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * This implementation provides:
 *   - a long-running supervisor reached over a UNIX domain socket
 *   - multi-container launch via clone() and Linux namespaces
 *   - per-container producer threads feeding a bounded log buffer
 *   - a logging consumer thread that persists logs to disk
 *   - supervisor-side metadata tracking, stop handling, and child reaping
 *   - integration with the kernel memory monitor through ioctl
 */

#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define STOP_GRACE_SECONDS 3

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_HARD_LIMIT_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record container_record_t;
typedef struct supervisor_ctx supervisor_ctx_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int stop_requested;
    int force_kill_sent;
    int monitor_registered;
    int run_client_fd;
    int producer_thread_started;
    int producer_thread_joined;
    pthread_t producer_thread;
    time_t stop_deadline;
    container_record_t *next;
};

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_thread_arg_t;

struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    char base_rootfs[PATH_MAX];
};

static volatile sig_atomic_t g_sigchld_received = 0;
static volatile sig_atomic_t g_shutdown_requested = 0;
static volatile sig_atomic_t g_run_client_signal = 0;

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item);
void *logging_thread(void *arg);
int child_fn(void *arg);
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes);
int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid);

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int validate_container_id(const char *id)
{
    size_t i;

    if (!id || id[0] == '\0')
        return -1;

    for (i = 0; id[i] != '\0'; i++) {
        unsigned char ch = (unsigned char)id[i];

        if (!isalnum(ch) && ch != '-' && ch != '_')
            return -1;
    }

    return 0;
}

static int set_cloexec(int fd)
{
    int flags;

    flags = fcntl(fd, F_GETFD);
    if (flags < 0)
        return -1;

    if (fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0)
        return -1;

    return 0;
}

static ssize_t read_full(int fd, void *buf, size_t len)
{
    size_t done = 0;
    char *ptr = buf;

    while (done < len) {
        ssize_t rc = read(fd, ptr + done, len - done);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (rc == 0)
            return (ssize_t)done;

        done += (size_t)rc;
    }

    return (ssize_t)done;
}

static int write_full(int fd, const void *buf, size_t len)
{
    size_t done = 0;
    const char *ptr = buf;

    while (done < len) {
        ssize_t rc = write(fd, ptr + done, len - done);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (rc == 0) {
            errno = EPIPE;
            return -1;
        }

        done += (size_t)rc;
    }

    return 0;
}

static int send_response(int fd, int status, const char *fmt, ...)
{
    control_response_t resp;
    va_list ap;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;

    if (fmt && fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(resp.message, sizeof(resp.message), fmt, ap);
        va_end(ap);
    }

    return write_full(fd, &resp, sizeof(resp));
}

static int is_container_active(container_state_t state)
{
    return state == CONTAINER_STARTING || state == CONTAINER_RUNNING;
}

static int container_final_status(const container_record_t *record)
{
    if (record->exit_signal > 0)
        return 128 + record->exit_signal;

    if (record->exit_code >= 0)
        return record->exit_code;

    return 1;
}

static container_record_t *find_container_by_id_locked(supervisor_ctx_t *ctx,
                                                       const char *id)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx,
                                                        pid_t pid)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (cur->host_pid == pid)
            return cur;
    }

    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (!is_container_active(cur->state))
            continue;

        if (strncmp(cur->rootfs, rootfs, sizeof(cur->rootfs)) == 0)
            return 1;
    }

    return 0;
}

static int active_container_count_locked(supervisor_ctx_t *ctx)
{
    int active = 0;
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (is_container_active(cur->state))
            active++;
    }

    return active;
}

static void format_timestamp(time_t when, char *buf, size_t buf_len)
{
    struct tm tm_buf;

    if (when == 0) {
        snprintf(buf, buf_len, "-");
        return;
    }

    if (!localtime_r(&when, &tm_buf)) {
        snprintf(buf, buf_len, "-");
        return;
    }

    if (strftime(buf, buf_len, "%Y-%m-%d %H:%M:%S", &tm_buf) == 0)
        snprintf(buf, buf_len, "-");
}

static int write_ps_payload(supervisor_ctx_t *ctx, int client_fd)
{
    char line[1024];
    container_record_t *cur;
    int have_any = 0;

    snprintf(line,
             sizeof(line),
             "ID\tPID\tSTATE\tSTARTED\tSOFT_MIB\tHARD_MIB\tEXIT\tSIGNAL\tNICE\tROOTFS\tCOMMAND\tLOG\n");
    if (write_full(client_fd, line, strlen(line)) < 0)
        return -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cur = ctx->containers; cur; cur = cur->next) {
        char started_at[64];

        have_any = 1;
        format_timestamp(cur->started_at, started_at, sizeof(started_at));
        snprintf(line,
                 sizeof(line),
                 "%s\t%d\t%s\t%s\t%lu\t%lu\t%d\t%d\t%d\t%s\t%s\t%s\n",
                 cur->id,
                 cur->host_pid,
                 state_to_string(cur->state),
                 started_at,
                 cur->soft_limit_bytes >> 20,
                 cur->hard_limit_bytes >> 20,
                 cur->exit_code,
                 cur->exit_signal,
                 cur->nice_value,
                 cur->rootfs,
                 cur->command,
                 cur->log_path);
        if (write_full(client_fd, line, strlen(line)) < 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            return -1;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!have_any) {
        snprintf(line, sizeof(line), "(no containers tracked)\n");
        if (write_full(client_fd, line, strlen(line)) < 0)
            return -1;
    }

    return 0;
}

static int stream_logs_payload(supervisor_ctx_t *ctx,
                               const char *container_id,
                               int client_fd)
{
    container_record_t *record;
    char log_path[PATH_MAX];
    char buf[LOG_CHUNK_SIZE];
    int fd;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        errno = ENOENT;
        return -1;
    }

    strncpy(log_path, record->log_path, sizeof(log_path) - 1);
    log_path[sizeof(log_path) - 1] = '\0';
    pthread_mutex_unlock(&ctx->metadata_lock);

    fd = open(log_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;

    for (;;) {
        ssize_t rc = read(fd, buf, sizeof(buf));

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            close(fd);
            return -1;
        }

        if (rc == 0)
            break;

        if (write_full(client_fd, buf, (size_t)rc) < 0) {
            close(fd);
            return -1;
        }
    }

    close(fd);
    return 0;
}

static void *producer_thread_main(void *arg)
{
    producer_thread_arg_t *thread_arg = arg;
    log_item_t item;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id,
            thread_arg->container_id,
            sizeof(item.container_id) - 1);

    for (;;) {
        ssize_t rc = read(thread_arg->read_fd, item.data, sizeof(item.data));

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("read container log pipe");
            break;
        }

        if (rc == 0)
            break;

        item.length = (size_t)rc;
        if (bounded_buffer_push(&thread_arg->ctx->log_buffer, &item) != 0)
            break;
    }

    close(thread_arg->read_fd);
    free(thread_arg);
    return NULL;
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return 1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    for (;;) {
        char log_path[PATH_MAX];
        container_record_t *record;
        int fd;
        size_t written = 0;

        memset(&item, 0, sizeof(item));
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id_locked(ctx, item.container_id);
        if (!record) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            continue;
        }

        strncpy(log_path, record->log_path, sizeof(log_path) - 1);
        log_path[sizeof(log_path) - 1] = '\0';
        pthread_mutex_unlock(&ctx->metadata_lock);

        fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
        if (fd < 0) {
            perror("open log file");
            continue;
        }

        while (written < item.length) {
            ssize_t rc = write(fd, item.data + written, item.length - written);

            if (rc < 0) {
                if (errno == EINTR)
                    continue;
                perror("write log file");
                break;
            }

            if (rc == 0) {
                errno = EIO;
                perror("write log file");
                break;
            }

            written += (size_t)rc;
        }

        close(fd);
    }

    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    int stdin_fd;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }

    if (cfg->log_write_fd > STDERR_FILENO)
        close(cfg->log_write_fd);

    stdin_fd = open("/dev/null", O_RDONLY);
    if (stdin_fd < 0) {
        perror("open /dev/null");
        return 1;
    }

    if (dup2(stdin_fd, STDIN_FILENO) < 0) {
        perror("dup2 stdin");
        close(stdin_fd);
        return 1;
    }
    close(stdin_fd);

    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0) {
        perror("setpriority");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount MS_PRIVATE");
        return 1;
    }

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    execl("/bin/sh", "/bin/sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int connect_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    if (set_cloexec(fd) < 0) {
        perror("fcntl FD_CLOEXEC");
        close(fd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }

    return fd;
}

static int create_server_socket(void)
{
    int fd;
    int probe_fd = -1;
    struct sockaddr_un addr;

    if (access(CONTROL_PATH, F_OK) == 0) {
        probe_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (probe_fd >= 0) {
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

            if (connect(probe_fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
                fprintf(stderr, "Supervisor already running at %s\n", CONTROL_PATH);
                close(probe_fd);
                errno = EADDRINUSE;
                return -1;
            }

            close(probe_fd);
        }

        if (unlink(CONTROL_PATH) < 0 && errno != ENOENT) {
            perror("unlink stale control socket");
            return -1;
        }
    }

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    if (set_cloexec(fd) < 0) {
        perror("fcntl FD_CLOEXEC");
        close(fd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, 16) < 0) {
        perror("listen");
        close(fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    return fd;
}

static int accept_client_socket(int server_fd)
{
    int fd;

#ifdef SOCK_CLOEXEC
    fd = accept4(server_fd, NULL, NULL, SOCK_CLOEXEC);
    if (fd >= 0)
        return fd;
    if (errno != ENOSYS && errno != EINVAL)
        return -1;
#endif

    fd = accept(server_fd, NULL, NULL);
    if (fd < 0)
        return -1;

    if (set_cloexec(fd) < 0) {
        perror("fcntl FD_CLOEXEC");
        close(fd);
        return -1;
    }

    return fd;
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGCHLD) {
        g_sigchld_received = 1;
        return;
    }

    if (signo == SIGINT || signo == SIGTERM)
        g_shutdown_requested = 1;
}

static void run_client_signal_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM)
        g_run_client_signal = 1;
}

static int install_supervisor_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = supervisor_signal_handler;

    if (sigaction(SIGCHLD, &sa, NULL) < 0) {
        perror("sigaction SIGCHLD");
        return -1;
    }

    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction SIGINT");
        return -1;
    }

    if (sigaction(SIGTERM, &sa, NULL) < 0) {
        perror("sigaction SIGTERM");
        return -1;
    }

    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        perror("sigaction SIGPIPE");
        return -1;
    }

    return 0;
}

static int install_run_client_signal_handlers(struct sigaction *old_int,
                                              struct sigaction *old_term,
                                              struct sigaction *old_pipe)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = run_client_signal_handler;

    if (sigaction(SIGINT, &sa, old_int) < 0) {
        perror("sigaction SIGINT");
        return -1;
    }

    if (sigaction(SIGTERM, &sa, old_term) < 0) {
        perror("sigaction SIGTERM");
        sigaction(SIGINT, old_int, NULL);
        return -1;
    }

    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, old_pipe) < 0) {
        perror("sigaction SIGPIPE");
        sigaction(SIGINT, old_int, NULL);
        sigaction(SIGTERM, old_term, NULL);
        return -1;
    }

    g_run_client_signal = 0;
    return 0;
}

static void restore_run_client_signal_handlers(const struct sigaction *old_int,
                                               const struct sigaction *old_term,
                                               const struct sigaction *old_pipe)
{
    sigaction(SIGINT, old_int, NULL);
    sigaction(SIGTERM, old_term, NULL);
    sigaction(SIGPIPE, old_pipe, NULL);
}

static int forward_stop_request_for_run(const char *container_id)
{
    control_request_t stop_req;
    control_response_t resp;
    int fd;

    memset(&stop_req, 0, sizeof(stop_req));
    stop_req.kind = CMD_STOP;
    strncpy(stop_req.container_id, container_id, sizeof(stop_req.container_id) - 1);

    fd = connect_control_socket();
    if (fd < 0)
        return -1;

    if (write_full(fd, &stop_req, sizeof(stop_req)) < 0) {
        perror("write stop request");
        close(fd);
        return -1;
    }

    if (read_full(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read stop response");
        close(fd);
        return -1;
    }

    close(fd);

    if (resp.status < 0) {
        if (resp.message[0] != '\0')
            fprintf(stderr, "%s\n", resp.message);
        return -1;
    }

    if (resp.message[0] != '\0')
        fprintf(stderr, "%s\n", resp.message);

    return 0;
}

static int receive_control_response(int fd,
                                    const control_request_t *req,
                                    control_response_t *resp)
{
    size_t done = 0;
    int stop_forwarded = 0;

    memset(resp, 0, sizeof(*resp));

    while (done < sizeof(*resp)) {
        ssize_t rc = read(fd, (char *)resp + done, sizeof(*resp) - done);

        if (rc < 0) {
            if (errno == EINTR) {
                if (req->kind == CMD_RUN && g_run_client_signal && !stop_forwarded) {
                    forward_stop_request_for_run(req->container_id);
                    stop_forwarded = 1;
                }
                continue;
            }

            perror("read control response");
            return -1;
        }

        if (rc == 0) {
            fprintf(stderr, "Supervisor closed the control connection unexpectedly\n");
            return -1;
        }

        done += (size_t)rc;
    }

    return 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           int run_client_fd,
                           control_response_t *resp)
{
    char resolved_rootfs[PATH_MAX];
    char log_path[PATH_MAX];
    void *child_stack = NULL;
    int log_pipe[2] = { -1, -1 };
    child_config_t child_cfg;
    pid_t child_pid = -1;
    int monitor_registered = 0;
    int log_fd = -1;
    int rc = 0;
    container_record_t *record = NULL;
    producer_thread_arg_t *thread_arg = NULL;

    memset(resp, 0, sizeof(*resp));

    if (validate_container_id(req->container_id) != 0) {
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Invalid container id '%s' (use letters, digits, '-' or '_')",
                 req->container_id);
        return -1;
    }

    if (!realpath(req->rootfs, resolved_rootfs)) {
        perror("realpath container rootfs");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to resolve rootfs path '%s': %s",
                 req->rootfs,
                 strerror(errno));
        return -1;
    }

    if (strncmp(resolved_rootfs, ctx->base_rootfs, sizeof(resolved_rootfs)) == 0) {
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Container rootfs must be a writable copy, not the base rootfs");
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id_locked(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Container id '%s' is already tracked",
                 req->container_id);
        return -1;
    }

    if (rootfs_in_use_locked(ctx, resolved_rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Rootfs '%s' is already in use by a running container",
                 resolved_rootfs);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to create log directory '%s': %s",
                 LOG_DIR,
                 strerror(errno));
        return -1;
    }

    if (snprintf(log_path,
                 sizeof(log_path),
                 "%s/%s.log",
                 LOG_DIR,
                 req->container_id) >= (int)sizeof(log_path)) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Log path is too long");
        return -1;
    }

    log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (log_fd < 0) {
        perror("open log file");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to create log file '%s': %s",
                 log_path,
                 strerror(errno));
        return -1;
    }
    close(log_fd);
    log_fd = -1;

    if (pipe2(log_pipe, O_CLOEXEC) < 0) {
        perror("pipe2");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to create log pipe: %s",
                 strerror(errno));
        return -1;
    }

    child_stack = malloc(STACK_SIZE);
    if (!child_stack) {
        perror("malloc child stack");
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Unable to allocate child stack");
        goto fail;
    }

    memset(&child_cfg, 0, sizeof(child_cfg));
    strncpy(child_cfg.id, req->container_id, sizeof(child_cfg.id) - 1);
    strncpy(child_cfg.rootfs, resolved_rootfs, sizeof(child_cfg.rootfs) - 1);
    strncpy(child_cfg.command, req->command, sizeof(child_cfg.command) - 1);
    child_cfg.nice_value = req->nice_value;
    child_cfg.log_write_fd = log_pipe[1];

    child_pid = clone(child_fn,
                      (char *)child_stack + STACK_SIZE,
                      CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNET | SIGCHLD,
                      &child_cfg);
    if (child_pid < 0) {
        perror("clone");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "clone() failed for container '%s': %s",
                 req->container_id,
                 strerror(errno));
        goto fail;
    }

    close(log_pipe[1]);
    log_pipe[1] = -1;

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd,
                                  req->container_id,
                                  child_pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) < 0) {
            perror("ioctl MONITOR_REGISTER");
            resp->status = -1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Unable to register container '%s' with monitor: %s",
                     req->container_id,
                     strerror(errno));
            goto fail;
        }
        monitor_registered = 1;
    }

    record = calloc(1, sizeof(*record));
    if (!record) {
        perror("calloc container record");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to allocate container metadata");
        goto fail;
    }

    record->host_pid = child_pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code = -1;
    record->exit_signal = 0;
    record->nice_value = req->nice_value;
    record->monitor_registered = 1;
    record->run_client_fd = run_client_fd;
    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    strncpy(record->log_path, log_path, sizeof(record->log_path) - 1);
    strncpy(record->rootfs, resolved_rootfs, sizeof(record->rootfs) - 1);
    strncpy(record->command, req->command, sizeof(record->command) - 1);

    thread_arg = calloc(1, sizeof(*thread_arg));
    if (!thread_arg) {
        perror("calloc producer thread arg");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to allocate producer thread argument");
        goto fail;
    }

    thread_arg->ctx = ctx;
    thread_arg->read_fd = log_pipe[0];
    strncpy(thread_arg->container_id,
            req->container_id,
            sizeof(thread_arg->container_id) - 1);

    rc = pthread_create(&record->producer_thread,
                        NULL,
                        producer_thread_main,
                        thread_arg);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create producer");
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to start log producer thread");
        goto fail;
    }

    record->producer_thread_started = 1;
    log_pipe[0] = -1;
    thread_arg = NULL;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    free(child_stack);

    resp->status = 0;
    snprintf(resp->message,
             sizeof(resp->message),
             "Container '%s' started with pid %d",
             req->container_id,
             child_pid);
    return 0;

fail:
    if (thread_arg)
        free(thread_arg);

    if (record)
        free(record);

    if (monitor_registered) {
        if (unregister_from_monitor(ctx->monitor_fd, req->container_id, child_pid) < 0 &&
            errno != ENOENT)
            perror("ioctl MONITOR_UNREGISTER");
    }

    if (child_pid > 0) {
        if (kill(child_pid, SIGKILL) < 0 && errno != ESRCH)
            perror("kill SIGKILL");
        for (;;) {
            pid_t rc = waitpid(child_pid, NULL, 0);
            if (rc >= 0)
                break;
            if (errno != EINTR) {
                perror("waitpid");
                break;
            }
        }
    }

    if (log_pipe[0] >= 0)
        close(log_pipe[0]);
    if (log_pipe[1] >= 0)
        close(log_pipe[1]);
    if (log_fd >= 0)
        close(log_fd);
    free(child_stack);
    return -1;
}

static int stop_container(supervisor_ctx_t *ctx,
                          const char *container_id,
                          control_response_t *resp)
{
    container_record_t *record;
    pid_t pid_to_signal = -1;

    memset(resp, 0, sizeof(*resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unknown container '%s'",
                 container_id);
        return -1;
    }

    if (!is_container_active(record->state)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Container '%s' is not running",
                 container_id);
        return -1;
    }

    if (!record->stop_requested) {
        record->stop_requested = 1;
        record->force_kill_sent = 0;
        record->stop_deadline = time(NULL) + STOP_GRACE_SECONDS;
        pid_to_signal = record->host_pid;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid_to_signal > 0) {
        if (kill(pid_to_signal, SIGTERM) < 0 && errno != ESRCH) {
            perror("kill SIGTERM");
            resp->status = -1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Failed to signal container '%s': %s",
                     container_id,
                     strerror(errno));
            return -1;
        }
    }

    resp->status = 0;
    snprintf(resp->message,
             sizeof(resp->message),
             "Stop requested for container '%s'",
             container_id);
    return 0;
}

static void join_finished_producers(supervisor_ctx_t *ctx)
{
    for (;;) {
        container_record_t *record;
        pthread_t thread;
        int have_thread = 0;

        pthread_mutex_lock(&ctx->metadata_lock);
        for (record = ctx->containers; record; record = record->next) {
            if (!record->producer_thread_started || record->producer_thread_joined)
                continue;
            if (is_container_active(record->state))
                continue;

            record->producer_thread_joined = 1;
            thread = record->producer_thread;
            have_thread = 1;
            break;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!have_thread)
            break;

        pthread_join(thread, NULL);
    }
}

static void enforce_stop_deadlines(supervisor_ctx_t *ctx)
{
    container_record_t *record;
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record; record = record->next) {
        if (!is_container_active(record->state))
            continue;
        if (!record->stop_requested || record->force_kill_sent)
            continue;
        if (record->stop_deadline > now)
            continue;

        if (kill(record->host_pid, SIGKILL) < 0 && errno != ESRCH)
            perror("kill SIGKILL");
        record->force_kill_sent = 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void request_stop_for_all(supervisor_ctx_t *ctx)
{
    container_record_t *record;
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record; record = record->next) {
        if (!is_container_active(record->state))
            continue;

        if (!record->stop_requested) {
            record->stop_requested = 1;
            record->force_kill_sent = 0;
            record->stop_deadline = now + STOP_GRACE_SECONDS;

            if (kill(record->host_pid, SIGTERM) < 0 && errno != ESRCH)
                perror("kill SIGTERM");
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void reap_children(supervisor_ctx_t *ctx)
{
    for (;;) {
        int status = 0;
        pid_t child_pid = waitpid(-1, &status, WNOHANG);

        if (child_pid == 0)
            break;

        if (child_pid < 0) {
            if (errno == EINTR)
                continue;
            if (errno != ECHILD)
                perror("waitpid");
            break;
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *record = find_container_by_pid_locked(ctx, child_pid);

            if (record) {
                int run_client_fd = record->run_client_fd;
                int final_status;
                char message[CONTROL_MESSAGE_LEN];
                int unregister_needed = record->monitor_registered;
                int saved_errno = 0;

                if (WIFEXITED(status)) {
                    record->exit_code = WEXITSTATUS(status);
                    record->exit_signal = 0;
                    record->state = record->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    record->exit_code = -1;
                    record->exit_signal = WTERMSIG(status);
                    if (record->stop_requested) {
                        record->state = CONTAINER_STOPPED;
                    } else if (record->exit_signal == SIGKILL) {
                        record->state = CONTAINER_HARD_LIMIT_KILLED;
                    } else {
                        record->state = CONTAINER_KILLED;
                    }
                } else {
                    record->exit_code = -1;
                    record->exit_signal = 0;
                    record->state = record->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
                }

                record->monitor_registered = 0;
                record->run_client_fd = -1;
                final_status = container_final_status(record);
                snprintf(message,
                         sizeof(message),
                         "Container '%s' finished: state=%s exit=%d signal=%d",
                         record->id,
                         state_to_string(record->state),
                         record->exit_code,
                         record->exit_signal);
                pthread_mutex_unlock(&ctx->metadata_lock);

                if (unregister_needed && ctx->monitor_fd >= 0) {
                    if (unregister_from_monitor(ctx->monitor_fd, record->id, child_pid) < 0 &&
                        errno != ENOENT) {
                        saved_errno = errno;
                        perror("ioctl MONITOR_UNREGISTER");
                        errno = saved_errno;
                    }
                }

                if (run_client_fd >= 0) {
                    if (send_response(run_client_fd, final_status, "%s", message) < 0 &&
                        errno != EPIPE)
                        perror("write run response");
                    close(run_client_fd);
                }

                continue;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    g_sigchld_received = 0;
}

static void free_container_records(supervisor_ctx_t *ctx)
{
    container_record_t *cur = ctx->containers;

    while (cur) {
        container_record_t *next = cur->next;

        if (cur->run_client_fd >= 0)
            close(cur->run_client_fd);

        free(cur);
        cur = next;
    }

    ctx->containers = NULL;
}

static void handle_client_request(supervisor_ctx_t *ctx,
                                  int client_fd,
                                  const control_request_t *req)
{
    control_response_t resp;

    switch (req->kind) {
    case CMD_START:
        if (start_container(ctx, req, -1, &resp) == 0) {
            if (send_response(client_fd, 0, "%s", resp.message) < 0)
                perror("write start response");
        } else if (send_response(client_fd, -1, "%s", resp.message) < 0) {
            perror("write start error");
        }
        close(client_fd);
        return;

    case CMD_RUN:
        if (start_container(ctx, req, client_fd, &resp) != 0) {
            if (send_response(client_fd, -1, "%s", resp.message) < 0)
                perror("write run error");
            close(client_fd);
        }
        return;

    case CMD_PS:
        if (send_response(client_fd, 0, "ok") < 0) {
            perror("write ps response");
            close(client_fd);
            return;
        }
        if (write_ps_payload(ctx, client_fd) < 0 && errno != EPIPE)
            perror("write ps payload");
        close(client_fd);
        return;

    case CMD_LOGS:
    {
        container_record_t *record;
        char log_path[PATH_MAX];
        int log_fd;

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id_locked(ctx, req->container_id);
        if (!record) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            if (send_response(client_fd,
                              -1,
                              "Unknown container '%s'",
                              req->container_id) < 0)
                perror("write logs error");
            close(client_fd);
            return;
        }

        strncpy(log_path, record->log_path, sizeof(log_path) - 1);
        log_path[sizeof(log_path) - 1] = '\0';
        pthread_mutex_unlock(&ctx->metadata_lock);

        log_fd = open(log_path, O_RDONLY | O_CLOEXEC);
        if (log_fd < 0) {
            if (send_response(client_fd,
                              -1,
                              "Unable to read logs for '%s': %s",
                              req->container_id,
                              strerror(errno)) < 0)
                perror("write logs error");
            close(client_fd);
            return;
        }
        close(log_fd);

        if (send_response(client_fd, 0, "ok") < 0) {
            perror("write logs response");
            close(client_fd);
            return;
        }
        if (stream_logs_payload(ctx, req->container_id, client_fd) < 0 &&
            errno != EPIPE)
            perror("write logs payload");
        close(client_fd);
        return;
    }

    case CMD_STOP:
        if (stop_container(ctx, req->container_id, &resp) == 0) {
            if (send_response(client_fd, 0, "%s", resp.message) < 0)
                perror("write stop response");
        } else if (send_response(client_fd, -1, "%s", resp.message) < 0) {
            perror("write stop error");
        }
        close(client_fd);
        return;

    default:
        if (send_response(client_fd, -1, "Unknown command kind: %d", req->kind) < 0)
            perror("write unknown command response");
        close(client_fd);
        return;
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    int active = 0;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    if (!realpath(rootfs, ctx.base_rootfs)) {
        perror("realpath base rootfs");
        return 1;
    }

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "[supervisor] WARNING: open /dev/container_monitor: %s\n"
                "[supervisor] Memory monitoring disabled. Load monitor.ko first for full functionality.\n",
                strerror(errno));
    }

    ctx.server_fd = create_server_socket();
    if (ctx.server_fd < 0) {
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (install_supervisor_signal_handlers() < 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    while (1) {
        fd_set read_fds;
        struct timeval timeout;

        reap_children(&ctx);
        if (g_shutdown_requested && !ctx.should_stop) {
            ctx.should_stop = 1;
            if (ctx.server_fd >= 0) {
                close(ctx.server_fd);
                ctx.server_fd = -1;
                unlink(CONTROL_PATH);
            }
            request_stop_for_all(&ctx);
        }

        if (ctx.should_stop)
            enforce_stop_deadlines(&ctx);

        join_finished_producers(&ctx);

        pthread_mutex_lock(&ctx.metadata_lock);
        active = active_container_count_locked(&ctx);
        pthread_mutex_unlock(&ctx.metadata_lock);

        if (ctx.should_stop && active == 0)
            break;

        if (ctx.server_fd < 0) {
            usleep(100000);
            continue;
        }

        FD_ZERO(&read_fds);
        FD_SET(ctx.server_fd, &read_fds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        rc = select(ctx.server_fd + 1, &read_fds, NULL, NULL, &timeout);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            g_shutdown_requested = 1;
            continue;
        }

        if (rc == 0)
            continue;

        if (FD_ISSET(ctx.server_fd, &read_fds)) {
            int client_fd = accept_client_socket(ctx.server_fd);

            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                continue;
            }

            memset(&read_fds, 0, sizeof(read_fds));
            {
                control_request_t req;
                ssize_t got = read_full(client_fd, &req, sizeof(req));

                if (got < 0) {
                    perror("read control request");
                    close(client_fd);
                    continue;
                }

                if (got != (ssize_t)sizeof(req)) {
                    fprintf(stderr, "Short control request received\n");
                    close(client_fd);
                    continue;
                }

                handle_client_request(&ctx, client_fd, &req);
            }
        }
    }

    request_stop_for_all(&ctx);
    reap_children(&ctx);
    join_finished_producers(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    free_container_records(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    control_response_t resp;
    struct sigaction old_int;
    struct sigaction old_term;
    struct sigaction old_pipe;
    int fd;
    int rc = 1;
    int handlers_installed = 0;

    fd = connect_control_socket();
    if (fd < 0)
        return 1;

    if (req->kind == CMD_RUN) {
        if (install_run_client_signal_handlers(&old_int, &old_term, &old_pipe) < 0) {
            close(fd);
            return 1;
        }
        handlers_installed = 1;
    }

    if (write_full(fd, req, sizeof(*req)) < 0) {
        perror("write control request");
        goto out;
    }

    if (receive_control_response(fd, req, &resp) < 0)
        goto out;

    if (resp.status < 0) {
        if (resp.message[0] != '\0')
            fprintf(stderr, "%s\n", resp.message);
        goto out;
    }

    if (req->kind == CMD_PS || req->kind == CMD_LOGS) {
        char buf[LOG_CHUNK_SIZE];

        for (;;) {
            ssize_t n = read(fd, buf, sizeof(buf));

            if (n < 0) {
                if (errno == EINTR)
                    continue;
                perror("read control payload");
                goto out;
            }

            if (n == 0)
                break;

            if (write_full(STDOUT_FILENO, buf, (size_t)n) < 0) {
                perror("write stdout");
                goto out;
            }
        }

        rc = 0;
        goto out;
    }

    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);

    if (req->kind == CMD_RUN) {
        rc = resp.status;
    } else {
        rc = 0;
    }

out:
    if (handlers_installed)
        restore_run_client_signal_handlers(&old_int, &old_term, &old_pipe);
    close(fd);
    return rc;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
