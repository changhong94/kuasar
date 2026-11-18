// SPDX-License-Identifier: Apache-2.0
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <grp.h>

int main(void) {
    char cwd[4096];
    gid_t groups[256];
    int n = getgroups(256, groups);
    printf("%u:%u:%s:%s:%d", (unsigned)getuid(), (unsigned)getgid(),
           getcwd(cwd, sizeof cwd), getenv("MARKER"), n);
    for (int i = 0; i < n; i++) {
        printf(":%u", (unsigned)groups[i]);
    }
    puts("");
    fflush(stdout);
    usleep(100000);
    return 7;
}
