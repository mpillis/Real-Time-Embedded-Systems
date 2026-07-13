#include <libwebsockets.h>
#include <cjson/cJSON.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

#define JETSTREAM_HOST "jetstream1.us-east.bsky.network"
#define JETSTREAM_PATH "/subscribe?wantedCollections=app.bsky.feed.post"
#define LOG_FILE       "metrics_log.txt"

#define RING_SLOTS 512
#define SLOT_SIZE  65536

static const uint32_t backoff_ms[] = { 1000, 2000, 4000, 8000 };
static const lws_retry_bo_t retry_policy = {
    .retry_ms_table          = backoff_ms,
    .retry_ms_table_count    = 4,
    .conceal_count           = 0,
    .secs_since_valid_ping   = 10,
    .secs_since_valid_hangup = 20,
};

static volatile sig_atomic_t g_stop = 0;

static char   ring[RING_SLOTS][SLOT_SIZE];
static size_t ring_len[RING_SLOTS];
static int    ring_head = 0, ring_tail = 0, ring_count = 0;
static int    ring_peak = 0;

static pthread_mutex_t buf_mutex     = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  buf_not_empty = PTHREAD_COND_INITIALIZER;

static pthread_mutex_t cnt_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned long c_commit = 0, c_identity = 0, c_account = 0, c_info = 0;

static unsigned long stat_received = 0, stat_dropped = 0, stat_oversized = 0;
static size_t stat_maxlen = 0;
static unsigned long stat_parse_err = 0, stat_connects = 0, stat_unknown = 0;

static char   asm_buf[SLOT_SIZE];
static size_t asm_len = 0;
static int    asm_overflow = 0;

static struct lws_context *g_context = NULL;
static struct lws         *g_wsi     = NULL;
static volatile int        g_connected = 0;

static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
}

static void ring_push(const char *data, size_t len) {
    pthread_mutex_lock(&buf_mutex);
    if (ring_count == RING_SLOTS) {
        stat_dropped++;
        pthread_mutex_unlock(&buf_mutex);
        return;
    }
    memcpy(ring[ring_head], data, len);
    ring_len[ring_head] = len;
    ring_head = (ring_head + 1) % RING_SLOTS;
    ring_count++;
    if (ring_count > ring_peak) ring_peak = ring_count;
    pthread_cond_signal(&buf_not_empty);
    pthread_mutex_unlock(&buf_mutex);
}

static int cb_jetstream(struct lws *wsi, enum lws_callback_reasons reason,
                        void *user, void *in, size_t len) {
    (void)user;

    switch (reason) {

    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        g_connected = 1;
        asm_len = 0;
        asm_overflow = 0;
        fprintf(stderr, "[net] connected\n");
        break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
        if (lws_is_first_fragment(wsi)) {
            asm_len = 0;
            asm_overflow = 0;
        }
        if (!asm_overflow) {
            if (asm_len + len < SLOT_SIZE) {
                memcpy(asm_buf + asm_len, in, len);
                asm_len += len;
            } else {
                asm_overflow = 1;
            }
        }
        if (lws_is_final_fragment(wsi)) {
            if (asm_overflow) {
                stat_oversized++;
            } else if (asm_len > 0) {
                if (asm_len > stat_maxlen) stat_maxlen = asm_len;
                stat_received++;
                ring_push(asm_buf, asm_len);
            }
            asm_len = 0;
            asm_overflow = 0;
        }
        break;

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        g_connected = 0;
        g_wsi = NULL;
        fprintf(stderr, "[net] connect error: %s\n", in ? (char *)in : "(none)");
        break;

    case LWS_CALLBACK_CLIENT_CLOSED:
        g_connected = 0;
        g_wsi = NULL;
        fprintf(stderr, "[net] closed\n");
        break;

    default:
        break;
    }
    return 0;
}

static struct lws_protocols protocols[] = {
    { "jetstream-protocol", cb_jetstream, 0, SLOT_SIZE, 0, NULL, 0 },
    { NULL, NULL, 0, 0, 0, NULL, 0 }
};

static void *producer_thread(void *arg) {
    (void)arg;
    time_t next_try = 0;
    int backoff = 1;

    while (!g_stop) {
        if (g_connected) {
            backoff = 1;
        } else if (!g_wsi) {
            time_t now = time(NULL);
            if (now >= next_try) {
                struct lws_client_connect_info ci;
                memset(&ci, 0, sizeof(ci));
                ci.context             = g_context;
                ci.address             = JETSTREAM_HOST;
                ci.port                = 443;
                ci.path                = JETSTREAM_PATH;
                ci.host                = JETSTREAM_HOST;
                ci.origin              = JETSTREAM_HOST;
                ci.protocol            = NULL;
                ci.local_protocol_name = "jetstream-protocol";
                ci.alpn                = "http/1.1";
                ci.ssl_connection      = LCCSCF_USE_SSL;
                ci.retry_and_idle_policy = &retry_policy;

                stat_connects++;
                g_wsi = lws_client_connect_via_info(&ci);

                next_try = now + backoff;
                backoff = (backoff < 8) ? backoff * 2 : 8;
            }
        }
        lws_service(g_context, 50);
    }
    return NULL;
}

static void *consumer_thread(void *arg) {
    (void)arg;
    static char local[SLOT_SIZE];

    for (;;) {
        pthread_mutex_lock(&buf_mutex);
        while (ring_count == 0 && !g_stop)
            pthread_cond_wait(&buf_not_empty, &buf_mutex);

        if (ring_count == 0 && g_stop) {
            pthread_mutex_unlock(&buf_mutex);
            break;
        }

        size_t n = ring_len[ring_tail];
        memcpy(local, ring[ring_tail], n);
        ring_tail = (ring_tail + 1) % RING_SLOTS;
        ring_count--;
        pthread_mutex_unlock(&buf_mutex);

        local[n] = '\0';

        cJSON *root = cJSON_Parse(local);
        if (!root) {
            pthread_mutex_lock(&cnt_mutex);
            stat_parse_err++;
            pthread_mutex_unlock(&cnt_mutex);
            continue;
        }

        cJSON *kind = cJSON_GetObjectItemCaseSensitive(root, "kind");
        if (cJSON_IsString(kind) && kind->valuestring) {
            pthread_mutex_lock(&cnt_mutex);
            if      (!strcmp(kind->valuestring, "commit"))   c_commit++;
            else if (!strcmp(kind->valuestring, "identity")) c_identity++;
            else if (!strcmp(kind->valuestring, "account"))  c_account++;
            else if (!strcmp(kind->valuestring, "info"))     c_info++;
            else                                             stat_unknown++;
            pthread_mutex_unlock(&cnt_mutex);
        }
        cJSON_Delete(root);
    }
    return NULL;
}

static int read_cpu(unsigned long long *total, unsigned long long *idle) {
    FILE *f = fopen("/proc/stat", "r");
    if (!f) return -1;

    char line[512];
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return -1;
    }
    fclose(f);

    unsigned long long v[10] = {0};
    int k = sscanf(line, "cpu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                   &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]);
    if (k < 5) return -1;

    unsigned long long t = 0;
    for (int i = 0; i < 10; i++) t += v[i];

    *total = t;
    *idle  = v[3] + v[4];
    return 0;
}

static void *monitor_thread(void *arg) {
    (void)arg;

    FILE *log = fopen(LOG_FILE, "a");
    if (!log) {
        perror("fopen metrics_log.txt");
        g_stop = 1;
        return NULL;
    }

    unsigned long long prev_total = 0, prev_idle = 0;
    read_cpu(&prev_total, &prev_idle);

    struct timespec next;
    clock_gettime(CLOCK_REALTIME, &next);
    next.tv_sec += 1;
    next.tv_nsec = 0;

    while (!g_stop) {
        int r;
        do {
            r = clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &next, NULL);
        } while (r == EINTR && !g_stop);

        if (g_stop) break;

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);

        unsigned long n_commit, n_identity, n_account, n_info;
        pthread_mutex_lock(&cnt_mutex);
        n_commit   = c_commit;
        n_identity = c_identity;
        n_account  = c_account;
        n_info     = c_info;
        c_commit = c_identity = c_account = c_info = 0;
        pthread_mutex_unlock(&cnt_mutex);

        pthread_mutex_lock(&buf_mutex);
        int occ = ring_peak;
        ring_peak = ring_count;
        pthread_mutex_unlock(&buf_mutex);
        double occ_pct = 100.0 * (double)occ / (double)RING_SLOTS;

        double cpu_pct = 0.0;
        unsigned long long tot, idl;
        if (read_cpu(&tot, &idl) == 0) {
            unsigned long long dt = tot - prev_total;
            unsigned long long di = idl - prev_idle;
            if (dt > 0) cpu_pct = 100.0 * (double)(dt - di) / (double)dt;
            prev_total = tot;
            prev_idle  = idl;
        }

        fprintf(log, "%ld,%ld,%lu,%lu,%lu,%lu,%.2f,%.2f\n",
                (long)ts.tv_sec, (long)ts.tv_nsec,
                n_commit, n_identity, n_account, n_info,
                occ_pct, cpu_pct);
        fflush(log);

        next.tv_sec += 1;
    }

    fclose(log);
    return NULL;
}

int main(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    lws_set_log_level(LLL_ERR | LLL_WARN, NULL);

    struct lws_context_creation_info info;
    memset(&info, 0, sizeof(info));
    info.port      = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options   = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT |
                     LWS_SERVER_OPTION_DISABLE_IPV6;
    info.gid = -1;
    info.uid = -1;

    g_context = lws_create_context(&info);
    if (!g_context) {
        fprintf(stderr, "lws_create_context failed\n");
        return 1;
    }

    memset(ring, 0, sizeof(ring));
    fprintf(stderr, "[main] pre-faulted %zu MB ring\n", sizeof(ring) / (1024*1024));

    pthread_t tp, tc, tm;
    if (pthread_create(&tp, NULL, producer_thread, NULL) ||
        pthread_create(&tc, NULL, consumer_thread, NULL) ||
        pthread_create(&tm, NULL, monitor_thread,  NULL)) {
        fprintf(stderr, "pthread_create failed\n");
        return 1;
    }

    fprintf(stderr, "[main] running, writing to %s (Ctrl+C to stop)\n", LOG_FILE);

    while (!g_stop) {
        struct timespec s = { 0, 200000000L };
        nanosleep(&s, NULL);
    }

    pthread_mutex_lock(&buf_mutex);
    pthread_cond_broadcast(&buf_not_empty);
    pthread_mutex_unlock(&buf_mutex);

    pthread_join(tp, NULL);
    pthread_join(tc, NULL);
    pthread_join(tm, NULL);

    lws_context_destroy(g_context);

    fprintf(stderr, "\n=== SHUTDOWN SUMMARY ===\n");
    fprintf(stderr, "messages received  : %lu\n", stat_received);
    fprintf(stderr, "dropped (buf full) : %lu\n", stat_dropped);
    fprintf(stderr, "oversized (>%d B)  : %lu\n", SLOT_SIZE, stat_oversized);
    fprintf(stderr, "largest message    : %zu B\n", stat_maxlen);
    fprintf(stderr, "JSON parse errors  : %lu\n", stat_parse_err);
    fprintf(stderr, "unknown kind       : %lu\n", stat_unknown);
    fprintf(stderr, "connection attempts: %lu\n", stat_connects);

    return 0;
}
