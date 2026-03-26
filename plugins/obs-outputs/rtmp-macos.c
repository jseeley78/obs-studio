#ifdef __APPLE__
#include "rtmp-stream.h"
#include <errno.h>
#include <sys/socket.h>
#include <sys/event.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

/* TCP_NOTSENT_LOWAT: only report writable when unsent data in the
 * kernel TCP stack drops below this threshold.  This gives us direct
 * visibility into kernel-side buffering caused by congestion, letting
 * us apply back-pressure before the user-space buffer even fills. */
#define NOTSENT_LOWAT_VALUE 16384

static void fatal_sock_shutdown(struct rtmp_stream *stream)
{
	close(stream->rtmp.m_sb.sb_socket);
	stream->rtmp.m_sb.sb_socket = -1;
	stream->write_buf_len = 0;
	os_event_signal(stream->buffer_space_available_event);
}

static bool handle_socket_read(struct rtmp_stream *stream,
			       uint64_t last_send_time)
{
	char discard[16384];

	for (;;) {
		int ret = recv(stream->rtmp.m_sb.sb_socket, discard,
			       sizeof(discard), 0);
		if (ret == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;

			blog(LOG_ERROR,
			     "socket_thread_macos: "
			     "Socket error, recv() returned "
			     "%d, errno %d",
			     ret, errno);
			stream->rtmp.last_error_code = errno;
			fatal_sock_shutdown(stream);
			return false;
		} else if (ret == 0) {
			if (last_send_time) {
				uint32_t diff = (os_gettime_ns() / 1000000) -
						last_send_time;
				blog(LOG_ERROR,
				     "socket_thread_macos: "
				     "Remote closed connection, "
				     "%u ms since last send "
				     "(buffer: %d / %d)",
				     diff, stream->write_buf_len,
				     stream->write_buf_size);
			} else {
				blog(LOG_ERROR,
				     "socket_thread_macos: "
				     "Remote closed connection");
			}
			stream->rtmp.last_error_code = 0;
			fatal_sock_shutdown(stream);
			return false;
		}
	}

	return true;
}

static bool handle_socket_eof(struct rtmp_stream *stream,
			      uint64_t last_send_time)
{
	if (last_send_time) {
		uint32_t diff = (os_gettime_ns() / 1000000) - last_send_time;

		blog(LOG_ERROR,
		     "socket_thread_macos: Received EOF, "
		     "%u ms since last send (buffer: %d / %d)",
		     diff, stream->write_buf_len, stream->write_buf_size);
	}

	if (os_event_try(stream->stop_event) != EAGAIN)
		blog(LOG_ERROR,
		     "socket_thread_macos: Aborting due "
		     "to EOF during shutdown, %d bytes lost",
		     stream->write_buf_len);
	else
		blog(LOG_ERROR, "socket_thread_macos: Aborting due to EOF");

	fatal_sock_shutdown(stream);
	return false;
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
			     "socket_thread_macos: "
			     "Socket error, send() returned %d, "
			     "errno %d",
			     ret, errno);
			stream->rtmp.last_error_code = errno;
		} else if (ret == 0) {
			blog(LOG_ERROR,
			     "socket_thread_macos: "
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

static inline void socket_thread_macos_internal(struct rtmp_stream *stream)
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

	/* Set TCP_NOTSENT_LOWAT so kqueue only reports the socket as
	 * writable when unsent data in the kernel drops below the
	 * threshold.  This prevents us from blindly stuffing data into
	 * the kernel buffer during congestion, which would otherwise
	 * translate directly into stream delay. */
	int notsent_lowat = NOTSENT_LOWAT_VALUE;
	if (setsockopt(stream->rtmp.m_sb.sb_socket, IPPROTO_TCP,
		       TCP_NOTSENT_LOWAT, &notsent_lowat,
		       sizeof(notsent_lowat)) < 0) {
		blog(LOG_WARNING,
		     "socket_thread_macos: Failed to set "
		     "TCP_NOTSENT_LOWAT (errno %d), falling back "
		     "to default write notification behavior",
		     errno);
	} else {
		blog(LOG_INFO,
		     "socket_thread_macos: TCP_NOTSENT_LOWAT set "
		     "to %d bytes",
		     notsent_lowat);
	}

	int kq = kqueue();
	if (kq < 0) {
		blog(LOG_ERROR,
		     "socket_thread_macos: Failed to create "
		     "kqueue, errno %d",
		     errno);
		fatal_sock_shutdown(stream);
		return;
	}

	/* Register events:
	 *   [0] EVFILT_READ  - socket readable (incoming data / EOF)
	 *   [1] EVFILT_WRITE - socket writable (respects TCP_NOTSENT_LOWAT)
	 *   [2] EVFILT_USER  - wakeup from buffer producer */
	struct kevent changes[3];
	EV_SET(&changes[0], stream->rtmp.m_sb.sb_socket, EVFILT_READ,
	       EV_ADD | EV_CLEAR, 0, 0, NULL);
	EV_SET(&changes[1], stream->rtmp.m_sb.sb_socket, EVFILT_WRITE,
	       EV_ADD | EV_CLEAR, 0, 0, NULL);
	EV_SET(&changes[2], 1, EVFILT_USER, EV_ADD | EV_CLEAR, NOTE_FFNOR, 0,
	       NULL);

	if (kevent(kq, changes, 3, NULL, 0, NULL) < 0) {
		blog(LOG_ERROR,
		     "socket_thread_macos: Failed to register "
		     "kqueue events, errno %d",
		     errno);
		close(kq);
		fatal_sock_shutdown(stream);
		return;
	}

	/* Store kqueue fd so the producer can trigger EVFILT_USER */
	stream->kqueue_fd = kq;

	struct kevent events[3];
	struct timespec timeout = {.tv_sec = 0, .tv_nsec = 200000000}; /* 200ms */

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

		int nev = kevent(kq, NULL, 0, events, 3, &timeout);
		if (nev < 0) {
			if (errno == EINTR)
				continue;
			blog(LOG_ERROR,
			     "socket_thread_macos: Aborting due "
			     "to kevent() failure, errno %d",
			     errno);
			close(kq);
			stream->kqueue_fd = -1;
			fatal_sock_shutdown(stream);
			return;
		}

		for (int i = 0; i < nev; i++) {
			struct kevent *ev = &events[i];

			if (ev->filter == EVFILT_READ) {
				if (ev->flags & EV_EOF) {
					if (!handle_socket_eof(stream,
							       last_send_time)) {
						close(kq);
						stream->kqueue_fd = -1;
						return;
					}
				} else {
					if (!handle_socket_read(
						    stream, last_send_time)) {
						close(kq);
						stream->kqueue_fd = -1;
						return;
					}
				}
			} else if (ev->filter == EVFILT_WRITE) {
				can_write = true;
			} else if (ev->filter == EVFILT_USER) {
				/* Woken up by producer - try to write
				 * if we haven't already been told we can */
				if (!can_write)
					can_write = true;
			}
		}

		/* Also check on timeout if there's data to flush */
		if (nev == 0) {
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
					close(kq);
					stream->kqueue_fd = -1;
					return;
				case RET_CONTINUE:;
				}
			}
		}
	exit_write_loop:;
	}

	close(kq);
	stream->kqueue_fd = -1;

	blog(LOG_INFO, "socket_thread_macos: Normal exit");
}

void *socket_thread_macos(void *data)
{
	struct rtmp_stream *stream = data;
	socket_thread_macos_internal(stream);
	return NULL;
}
#endif
