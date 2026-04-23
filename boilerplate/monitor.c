/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * YOUR WORK: Fill in all sections marked // TODO.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/workqueue.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1: Define your linked-list node struct.
 *
 * Requirements:
 *   - track PID, container ID, soft limit, and hard limit
 *   - remember whether the soft-limit warning was already emitted
 *   - include `struct list_head` linkage
 * ============================================================== */
struct monitored_entry {
    pid_t pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    bool soft_limit_logged;
    char container_id[MONITOR_NAME_LEN];
    struct list_head list;
};


/* ==============================================================
 * TODO 2: Declare the global monitored list and a lock.
 *
 * Requirements:
 *   - shared across ioctl and timer code paths
 *   - protect insert, remove, and iteration safely
 *
 * You may choose either a mutex or a spinlock, but your README must
 * justify the choice in terms of the code paths you implemented.
 * ============================================================== */
static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);
static struct work_struct monitor_work;


/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 *
 * Log a warning when a process exceeds the soft limit.
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 *
 * Kill a process when it exceeds the hard limit.
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

static bool monitor_request_matches(const struct monitored_entry *entry,
                                    const struct monitor_request *req)
{
    bool pid_match = true;
    bool id_match = true;

    if (req->pid > 0)
        pid_match = (entry->pid == req->pid);

    if (req->container_id[0] != '\0')
        id_match = (strncmp(entry->container_id,
                            req->container_id,
                            MONITOR_NAME_LEN) == 0);

    return pid_match && id_match;
}

static void monitor_work_fn(struct work_struct *work)
{
    (void)work;

    for (;;) {
        struct monitored_entry *entry;
        struct monitored_entry *tmp;
        char container_id[MONITOR_NAME_LEN];
        pid_t pid = -1;
        unsigned long limit_bytes = 0;
        long rss_bytes = -1;
        bool emit_soft_event = false;
        bool emit_hard_event = false;

        mutex_lock(&monitored_lock);

        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            rss_bytes = get_rss_bytes(entry->pid);
            if (rss_bytes < 0) {
                printk(KERN_INFO
                       "[container_monitor] Removing stale entry container=%s pid=%d\n",
                       entry->container_id, entry->pid);
                list_del(&entry->list);
                kfree(entry);
                continue;
            }

            if (rss_bytes > (long)entry->hard_limit_bytes) {
                strscpy(container_id, entry->container_id, sizeof(container_id));
                pid = entry->pid;
                limit_bytes = entry->hard_limit_bytes;
                list_del(&entry->list);
                kfree(entry);
                emit_hard_event = true;
                break;
            }

            if (!entry->soft_limit_logged &&
                rss_bytes > (long)entry->soft_limit_bytes) {
                entry->soft_limit_logged = true;
                strscpy(container_id, entry->container_id, sizeof(container_id));
                pid = entry->pid;
                limit_bytes = entry->soft_limit_bytes;
                emit_soft_event = true;
                break;
            }
        }

        mutex_unlock(&monitored_lock);

        if (emit_hard_event) {
            kill_process(container_id, pid, limit_bytes, rss_bytes);
            continue;
        }

        if (emit_soft_event) {
            log_soft_limit_event(container_id, pid, limit_bytes, rss_bytes);
            continue;
        }

        break;
    }
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    (void)t;

    /* ==============================================================
     * TODO 3: Implement periodic monitoring.
     *
     * Requirements:
     *   - iterate through tracked entries safely
     *   - remove entries for exited processes
     *   - emit soft-limit warning once per entry
     *   - enforce hard limit and then remove the entry
     *   - avoid use-after-free while deleting during iteration
     * ============================================================== */
    schedule_work(&monitor_work);

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 *
 * Supported operations:
 *   - register a PID with soft + hard limits
 *   - unregister a PID when the runtime no longer needs tracking
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    struct monitored_entry *entry;
    struct monitored_entry *tmp;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER) {
        printk(KERN_ERR "[container_monitor] Unsupported ioctl cmd=%u\n", cmd);
        return -EINVAL;
    }

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req))) {
        printk(KERN_ERR "[container_monitor] copy_from_user failed for cmd=%u\n", cmd);
        return -EFAULT;
    }

    req.container_id[MONITOR_NAME_LEN - 1] = '\0';

    if (cmd == MONITOR_REGISTER) {
        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        /* ==============================================================
         * TODO 4: Add a monitored entry.
         *
         * Requirements:
         *   - allocate and initialize one node from req
         *   - validate allocation and limits
         *   - insert into the shared list under the chosen lock
         * ============================================================== */
        if (req.pid <= 0) {
            printk(KERN_ERR "[container_monitor] Invalid pid=%d for register\n", req.pid);
            return -EINVAL;
        }

        if (req.container_id[0] == '\0') {
            printk(KERN_ERR "[container_monitor] Empty container ID on register\n");
            return -EINVAL;
        }

        if (req.soft_limit_bytes == 0 || req.hard_limit_bytes == 0 ||
            req.soft_limit_bytes > req.hard_limit_bytes) {
            printk(KERN_ERR
                   "[container_monitor] Invalid limits for container=%s pid=%d soft=%lu hard=%lu\n",
                   req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);
            return -EINVAL;
        }

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) {
            printk(KERN_ERR
                   "[container_monitor] kmalloc failed for container=%s pid=%d\n",
                   req.container_id, req.pid);
            return -ENOMEM;
        }

        memset(entry, 0, sizeof(*entry));
        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        strscpy(entry->container_id, req.container_id, sizeof(entry->container_id));

        mutex_lock(&monitored_lock);
        list_for_each_entry(tmp, &monitored_list, list) {
            if (tmp->pid == entry->pid ||
                strncmp(tmp->container_id,
                        entry->container_id,
                        sizeof(tmp->container_id)) == 0) {
                mutex_unlock(&monitored_lock);
                printk(KERN_ERR
                       "[container_monitor] Duplicate registration for container=%s pid=%d\n",
                       entry->container_id, entry->pid);
                kfree(entry);
                return -EEXIST;
            }
        }
        list_add_tail(&entry->list, &monitored_list);
        mutex_unlock(&monitored_lock);

        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /* ==============================================================
     * TODO 5: Remove a monitored entry on explicit unregister.
     *
     * Requirements:
     *   - search by PID, container ID, or both
     *   - remove the matching entry safely if found
     *   - return status indicating whether a matching entry was removed
     * ============================================================== */
    if (req.pid <= 0 && req.container_id[0] == '\0') {
        printk(KERN_ERR "[container_monitor] Unregister requires pid and/or container ID\n");
        return -EINVAL;
    }

    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        if (!monitor_request_matches(entry, &req))
            continue;

        list_del(&entry->list);
        mutex_unlock(&monitored_lock);

        printk(KERN_INFO
               "[container_monitor] Removed container=%s pid=%d\n",
               entry->container_id, entry->pid);
        kfree(entry);
        return 0;
    }
    mutex_unlock(&monitored_lock);

    return -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    INIT_WORK(&monitor_work, monitor_work_fn);
    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    struct monitored_entry *entry;
    struct monitored_entry *tmp;

    del_timer_sync(&monitor_timer);
    cancel_work_sync(&monitor_work);

    /* ==============================================================
     * TODO 6: Free all remaining monitored entries.
     *
     * Requirements:
     *   - remove and free every list node safely
     *   - leave no leaked state on module unload
     * ============================================================== */
    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&monitored_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
