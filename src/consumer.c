// SPDX-License-Identifier: BSD-3-Clause
#include "consumer.h"
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{

	so_packet_t packet;

	while (1) {
		/*Extract packet from the shared ring buffer*/
		if (ring_buffer_dequeue(ctx->producer_rb, &packet, sizeof(so_packet_t)) == -1)
			return;
		/*process packet content*/
		int action = process_packet(&packet);
		unsigned long hash = packet_hash(&packet);
		/*Write to output file atomically*/
		pthread_mutex_lock(&ctx->log_mutex);
		fprintf(ctx->file, "%s %016lx %lu\n", RES_TO_STR(action), hash, packet.hdr.timestamp);
		pthread_mutex_unlock(&ctx->log_mutex);
	}

}

void *consumer(void *context)
{
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)context;

	consumer_thread(ctx);
	return NULL;
}
int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{

	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)calloc(1, sizeof(so_consumer_ctx_t));

	pthread_mutex_init(&ctx->log_mutex, NULL);
	ctx->producer_rb = rb;
	ctx->file = fopen(out_filename, "w");

	if (!ctx->file) {
		pthread_mutex_destroy(&ctx->log_mutex);
		free(ctx);
		return -1;
	}
	/*Launch consumer threads in a thread pool architecture*/
	for (int i = 0; i < num_consumers; i++) {
		pthread_create(&tids[i], NULL, consumer, ctx);
	}
	return num_consumers;
}
