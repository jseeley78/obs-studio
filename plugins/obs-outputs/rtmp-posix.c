#if !defined(_WIN32) && !defined(__APPLE__)
#include "rtmp-stream.h"
#include <errno.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

static void fatal_sock_shutdown(struct rtmp_stream *stream)
{
	close(stream->rtmp.m_sb.sb_socket);
	stream->rtmp.m_sb.sb_socket = -1;
	stream->write_buf_len = 0;
	os_event_signal(stream->buffer_space_available_event);
}

static bool socket_event(struct rtmp_stream *stream, bool *can_write,
			 uint64_t last_send_time, short revents)
{
	if (revents & POLLOUT)
		*can_write = true;

	if (revents & (POLLHUP | POLLERR)) {
		if (last_send_time) {
			uint32_t diff =
				(os_gettime_ns() / 1000000) - last_send_time;

			blog(LOG_ERROR,
			     "socket_thread_posix: Received "
			     "POLLHUP/POLLERR, %u ms since last send "
			     "(buffer: %d / %d)",
			     diff, stream->write_buf_len,
			     stream->write_buf_size);
		}

		if (os_event_try(stream->stop_event) != EAGAIN)
			blog(LOG_ERROR,
			     "socket_thread_posix: Aborting due "
			     "to POLLHUP/POLLERR during shutdown, "
			     "%d bytes lost",
			     stream->write_buf_len);
		else
			blog(LOG_ERROR,
			     "socket_thread_posix: Aborting due "
			     "to POLLHUP/POLLERR");

		fatal_sock_shutdown(stream);
		return false;
	}

	if (revents & POLLIN) {
		char discard[16384];

		for (;;) {
			int ret = recv(stream->rtmp.m_sb.sb_socket, discard,
				       sizeof(discard), 0);
			if (ret == -1) {
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					break;

				blog(LOG_ERROR,
				     "socket_thread_posix: "
				     "Socket error, recv() returned "
				     "%d, errno %d",
				     ret, errno);
				stream->rtmp.last_error_code = errno;
				fatal_sock_shutdown(stream);
				return false;
			} else if (ret == 0) {
				blog(LOG_ERROR,
				     "socket_thread_posix: "
				     "Socket closed by remote, "
				     "recv() returned 0");
				stream->rtmp.last_error_code = 0;
				fatal_sock_shutdown(stream);
				return false;
			}
		}
	}

	return true;
}

enum data_ret { RET_BREAK, RET_FATAL, RET_CONTINUE };

static inline size_t min_size(size_t a, size_t b)
{
	return a < b ? a : b;
}

static enum data_ret write_data(struct rtmp_stream *stream, bool *can_write,
				uint64_t *last_send_time,
				size_t latency_packet_size, int delay_time)
{
	bool exit_loop = false;

	pthread_mutex_lock(&stream->write_buf_mutex);

	if (!stream->write_buf_len) {
		pthread_mutex_unlock(&stream->write_buf_mutex);
		return RET_BREAK;
	}

	int ret;
	if (stream->low_latency_mode) {
		size_t send_len =
			min_size(latency_packet_size, stream->write_buf_len);

		ret = RTMPSockBuf_Send(&stream->rtmp.m_sb,
				       (const char *)stream->write_buf,
				       (int)send_len);
	} else {
		ret = RTMPSockBuf_Send(&stream->rtmp.m_sb,
				       (const char *)stream->write_buf,
				       (int)stream->write_buf_len);
	}

	if (ret > 0) {
		if (stream->write_buf_len - ret)
			memmove(stream->write_buf, stream->write_buf + ret,
				stream->write_buf_len - ret);
		stream->write_buf_len -= ret;

		*last_send_time = os_gettime_ns() / 1000000;

		os_event_signal(stream->buffer_space_available_event);
	} else {
		if (ret == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				*can_write = false;
				pthread_mutex_unlock(&stream->write_buf_mutex);
				return RET_BREAK;
			}

			blog(LOG_ERROR,
			     "socket_thread_posix: "
			     "Socket error, send() returned %d, "
			     "errno %d",
			     ret, errno);
			stream->rtmp.last_error_code = errno;
		} else if (ret == 0) {
			blog(LOG_ERROR,
			     "socket_thread_posix: "
			     "Socket error, send() returned 0");
			stream->rtmp.last_error_code = 0;
		}

		pthread_mutex_unlock(&stream->write_buf_mutex);
		fatal_sock_shutdown(stream);
		return RET_FATAL;
	}

	/* finish writing for now */
	if (stream->write_buf_len <= 1000)
		exit_loop = true;

	pthread_mutex_unlock(&stream->write_buf_mutex);

	if (delay_time)
		os_sleep_ms(delay_time);

	return exit_loop ? RET_BREAK : RET_CONTINUE;
}

#define LATENCY_FACTOR 20

static inline void socket_thread_posix_internal(struct rtmp_stream *stream)
{
	bool can_write = false;

	int delay_time;
	size_t latency_packet_size;
	uint64_t last_send_time = 0;

	if (stream->low_latency_mode) {
		delay_time = 1000 / LATENCY_FACTOR;
		latency_packet_size =
			stream->write_buf_size / (LATENCY_FACTOR - 2);
	} else {
		latency_packet_size = stream->write_buf_size;
		delay_time = 0;
	}

	/* Use a self-pipe to allow the buffer producer to wake us up
	 * immediately when new data is enqueued, instead of relying
	 * on a poll() timeout. */
	struct pollfd fds[2];
	fds[0].fd = stream->rtmp.m_sb.sb_socket;
	fds[0].events = POLLIN | POLLOUT;
	fds[1].fd = stream->notify_pipe[0];
	fds[1].events = POLLIN;

	for (;;) {
		if (os_event_try(stream->send_thread_signaled_exit) !=
		    EAGAIN) {
			pthread_mutex_lock(&stream->write_buf_mutex);
			if (stream->write_buf_len == 0) {
				pthread_mutex_unlock(&stream->write_buf_mutex);
				os_event_reset(stream->send_thread_signaled_exit);
				break;
			}

			pthread_mutex_unlock(&stream->write_buf_mutex);
		}

		int status = poll(fds, 2, 200);
		if (status < 0) {
			if (errno == EINTR)
				continue;
			blog(LOG_ERROR,
			     "socket_thread_posix: Aborting due "
			     "to poll() failure, errno %d",
			     errno);
			fatal_sock_shutdown(stream);
			return;
		}

		/* Drain the self-pipe */
		if (fds[1].revents & POLLIN) {
			char buf[64];
			while (read(stream->notify_pipe[0], buf,
				    sizeof(buf)) > 0)
				;
		}

		if (status > 0 && fds[0].revents) {
			if (!socket_event(stream, &can_write, last_send_time,
					  fds[0].revents))
				return;
		}

		/* Also try to write if we woke up from the self-pipe
		 * or from a timeout with data pending */
		if (!can_write && (fds[1].revents & POLLIN || status == 0)) {
			pthread_mutex_lock(&stream->write_buf_mutex);
			bool has_data = stream->write_buf_len > 0;
			pthread_mutex_unlock(&stream->write_buf_mutex);

			if (has_data)
				can_write = true;
		}

		if (can_write) {
			for (;;) {
				enum data_ret ret = write_data(
					stream, &can_write, &last_send_time,
					latency_packet_size, delay_time);

				switch (ret) {
				case RET_BREAK:
					goto exit_write_loop;
				case RET_FATAL:
					return;
				case RET_CONTINUE:;
				}
			}
		}
	exit_write_loop:;
	}

	blog(LOG_INFO, "socket_thread_posix: Normal exit");
}

void *socket_thread_posix(void *data)
{
	struct rtmp_stream *stream = data;
	socket_thread_posix_internal(stream);
	return NULL;
}
#endif
