/* SPDX-License-Identifier: Apache-2.0
 * Guest-only generic image mount helper. No image-specific VM preparation.
 * Failure is fatal: the host keeps the slot private and destroys it.
 */
#define _GNU_SOURCE
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

static long long mono_us(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec*1000000LL+t.tv_nsec/1000; }
static const char root[] = "/run/image-root";
static void fail(const char *what) { perror(what); exit(126); }
static void directory(const char *path) {
    struct stat st;
    if (mkdir(path, 0755) && errno != EEXIST) fail(path);
    if (lstat(path, &st) || !S_ISDIR(st.st_mode)) { errno = ENOTDIR; fail(path); }
}
static void bind(const char *source, const char *target, int readonly) {
    if (mount(source, target, NULL, MS_BIND, NULL)) fail(target);
    if (readonly && mount(NULL, target, NULL, MS_REMOUNT|MS_BIND|MS_RDONLY, NULL)) fail("readonly bind");
}
static void helper(const char *target) {
    int fd = open(target, O_WRONLY|O_CREAT|O_CLOEXEC|O_NOFOLLOW, 0755);
    if (fd < 0) fail(target);
    struct stat st;
    if (fstat(fd, &st) || !S_ISREG(st.st_mode)) { errno=EINVAL; fail(target); }
    close(fd);
    bind("/bin/busybox", target, 1);
}
int main(int argc, char **argv) {
    if (argc==2 && !strcmp(argv[1],"--version")) { puts("image-mount 1"); return 0; }
    if (argc!=2 || (strcmp(argv[1],"oci") && strcmp(argv[1],"prepare"))) return 2;
    long long began=mono_us(), ready, mounted, lower, overlay, helper_done, bindings;
    struct stat st;
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (stat("/dev/vdb", &st) || !S_ISBLK(st.st_mode)) {
        clock_gettime(CLOCK_MONOTONIC, &now);
        if ((now.tv_sec-start.tv_sec)*1000000000LL+now.tv_nsec-start.tv_nsec >= 5000000000LL) {
            errno=ETIMEDOUT; fail("application disk");
        }
        usleep(1000);
    }
    /* devtmpfs and sysfs may appear before the block device accepts opens.
     * Wait for usable capacity, not just the device node; keep the 5s bound.
     */
    for (;;) {
        unsigned long long bytes=0;
        int ready=open("/dev/vdb",O_RDONLY|O_CLOEXEC);
        int usable=ready>=0 && ioctl(ready,BLKGETSIZE64,&bytes)==0 && bytes>0;
        if (ready>=0) close(ready);
        if (usable) break;
        clock_gettime(CLOCK_MONOTONIC,&now);
        if ((now.tv_sec-start.tv_sec)*1000000000LL+now.tv_nsec-start.tv_nsec >= 5000000000LL) {
            errno=ETIMEDOUT; fail("application disk capacity");
        }
        usleep(1000);
    }
    char serial[32]={0};
    int fd=open("/sys/block/vdb/serial",O_RDONLY|O_CLOEXEC);
    if (fd<0) fail("disk serial");
    ssize_t n=read(fd,serial,sizeof(serial)-1); close(fd);
    if (n<0) fail("disk serial");
    serial[strcspn(serial,"\n")]=0;
    if (strcmp(serial,"app-root")) { errno=EINVAL; fail("unexpected application disk"); }
    directory("/run/image-lower"); directory("/run/image-upper");
    directory("/run/image-work"); directory(root);
    if (!strcmp(argv[1],"prepare")) {
        fd=open("/dev/vdb",O_RDONLY|O_CLOEXEC);
        if (fd<0) fail("application disk open");
        close(fd); return 0;
    }
    /* A preattached zero disk was probed by the kernel. Discard its cached
     * partition/superblock reads after the host commits the immutable image. */
    fd=open("/dev/vdb", O_RDONLY|O_CLOEXEC);
    if (fd<0 || ioctl(fd, BLKFLSBUF)) fail("invalidate application disk cache");
    unsigned char magic[4];
    if (pread(fd,magic,sizeof magic,1024)!=sizeof magic) fail("image superblock");
    const char *filesystem = !memcmp(magic,"\xe2\xe1\xf5\xe0",4)?"erofs":"ext4";
    close(fd);
    ready=mono_us();
    if (mount("/dev/vdb","/run/image-lower",filesystem,MS_RDONLY,NULL)) fail("image lower");
    lower=mono_us();
    if (mount("overlay",root,"overlay",0,"lowerdir=/run/image-lower,upperdir=/run/image-upper,workdir=/run/image-work")) fail("image overlay");
    overlay=mono_us();
    directory("/run/image-root/.__kuasar");
    if (!strcmp(argv[1],"oci") && (!lstat("/run/image-root/.__kuasar/busybox",&st) || errno!=ENOENT)) {
        errno=EEXIST; fail("reserved OCI helper path");
    }
    helper("/run/image-root/.__kuasar/busybox");
    helper_done=mono_us();
    directory("/run/image-root/dev"); directory("/run/image-root/proc"); directory("/run/image-root/sys");
    bind("/dev","/run/image-root/dev",0); bind("/proc","/run/image-root/proc",0); bind("/sys","/run/image-root/sys",0);
    bindings=mono_us();
    mounted=mono_us();
    fprintf(stderr,"{\"ready_us\":%lld,\"mount_us\":%lld,\"ext4_us\":%lld,\"overlay_us\":%lld,\"helper_us\":%lld,\"bind_us\":%lld,\"check_us\":%lld}\n",ready-began,mounted-ready,lower-ready,overlay-lower,helper_done-overlay,bindings-helper_done,mounted-bindings);
    return 0;
}
