/*
 * Generic segmenter
 * Copyright (c) 2011, Luca Barbato
 *
 * This file is part of Libav.
 *
 * Libav is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * Libav is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Libav; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <float.h>

#include "avformat.h"
#include "internal.h"

#include "libavutil/log.h"
#include "libavutil/opt.h"
#include "libavutil/avstring.h"
#include "libavutil/parseutils.h"
#include "libavutil/mathematics.h"

typedef struct {
    const AVClass *class;  /**< Class for private options. */
    int number;
    AVFormatContext *avf;
    char *format;          /**< Set by a private option. */
    char *list;            /**< Set by a private option. */
    char *valid_frames;    /**< Set by a private option. */
    const char *valid_frame_delimiter;
    float time;            /**< Set by a private option. */
    int  size;             /**< Set by a private option. */
    int  wrap;             /**< Set by a private option. */
    int64_t offset_time;
    int64_t recording_time;
    int has_video;
    AVIOContext *pb;
    int64_t next_valid_frame;
    char *next_valid_frame_ptr;
    int64_t frame_count;
    int observe_valid_frames;
} SegmentContext;

static int segment_start(AVFormatContext *s)
{
    SegmentContext *c = s->priv_data;
    AVFormatContext *oc = c->avf;
    int err = 0;

    if (c->wrap)
        c->number %= c->wrap;

    if (av_get_frame_filename(oc->filename, sizeof(oc->filename),
                              s->filename, c->number++) < 0)
        return AVERROR(EINVAL);

    if ((err = avio_open2(&oc->pb, oc->filename, AVIO_FLAG_WRITE,
                          &s->interrupt_callback, NULL)) < 0)
        return err;

    if (!oc->priv_data && oc->oformat->priv_data_size > 0) {
        oc->priv_data = av_mallocz(oc->oformat->priv_data_size);
        if (!oc->priv_data) {
            avio_close(oc->pb);
            return AVERROR(ENOMEM);
        }
        if (oc->oformat->priv_class) {
            *(const AVClass**)oc->priv_data = oc->oformat->priv_class;
            av_opt_set_defaults(oc->priv_data);
        }
    }

    if ((err = oc->oformat->write_header(oc)) < 0) {
        goto fail;
    }

    return 0;

fail:
    avio_close(oc->pb);
    av_freep(&oc->priv_data);

    return err;
}

static int segment_end(AVFormatContext *oc)
{
    int ret = 0;

    if (oc->oformat->write_trailer)
        ret = oc->oformat->write_trailer(oc);

    avio_close(oc->pb);
    if (oc->oformat->priv_class)
        av_opt_free(oc->priv_data);
    av_freep(&oc->priv_data);

    return ret;
}

static int seg_write_header(AVFormatContext *s)
{
    SegmentContext *seg = s->priv_data;
    AVFormatContext *oc;
    int ret, i;
    char* token;

    seg->number = 0;
    seg->offset_time = 0;
    seg->recording_time = seg->time * 1000000;
    seg->frame_count = 0;
    seg->valid_frame_delimiter = ",";
    seg->next_valid_frame_ptr = NULL;

    if (seg->valid_frames) {
        token = av_strtok(seg->valid_frames, seg->valid_frame_delimiter, &seg->next_valid_frame_ptr);
        if (token) {
            seg->next_valid_frame = strtol(token, NULL, 10);
        }
        seg->observe_valid_frames = 1;
    } else {
        seg->next_valid_frame = 0;
        seg->observe_valid_frames = 0;
    }


    oc = avformat_alloc_context();

    if (!oc)
        return AVERROR(ENOMEM);

    if (seg->list)
        if ((ret = avio_open2(&seg->pb, seg->list, AVIO_FLAG_WRITE,
                              &s->interrupt_callback, NULL)) < 0)
            goto fail;

    for (i = 0; i< s->nb_streams; i++)
        seg->has_video +=
            (s->streams[i]->codec->codec_type == AVMEDIA_TYPE_VIDEO);

    if (seg->has_video > 1)
        av_log(s, AV_LOG_WARNING,
               "More than a single video stream present, "
               "expect issues decoding it.\n");

    oc->oformat = av_guess_format(seg->format, s->filename, NULL);

    if (!oc->oformat) {
        ret = AVERROR_MUXER_NOT_FOUND;
        goto fail;
    }
    if (oc->oformat->flags & AVFMT_NOFILE) {
        av_log(s, AV_LOG_ERROR, "format %s not supported.\n",
               oc->oformat->name);
        ret = AVERROR(EINVAL);
        goto fail;
    }

    seg->avf = oc;

    oc->streams = s->streams;
    oc->nb_streams = s->nb_streams;

    if (av_get_frame_filename(oc->filename, sizeof(oc->filename),
                              s->filename, seg->number++) < 0) {
        ret = AVERROR(EINVAL);
        goto fail;
    }

    if ((ret = avio_open2(&oc->pb, oc->filename, AVIO_FLAG_WRITE,
                          &s->interrupt_callback, NULL)) < 0)
        goto fail;

    if ((ret = avformat_write_header(oc, NULL)) < 0) {
        avio_close(oc->pb);
        goto fail;
    }

    if (seg->list) {
        avio_printf(seg->pb, "%s\n", oc->filename);
        avio_flush(seg->pb);
    }

fail:
    if (ret) {
        if (oc) {
            oc->streams = NULL;
            oc->nb_streams = 0;
            avformat_free_context(oc);
        }
        if (seg->list)
            avio_close(seg->pb);
    }
    return ret;
}

static int seg_write_packet(AVFormatContext *s, AVPacket *pkt)
{
    SegmentContext *seg = s->priv_data;
    AVFormatContext *oc = seg->avf;
    AVStream *st = oc->streams[pkt->stream_index];
    int64_t end_pts = seg->recording_time * seg->number;
    int ret;
    char* token;
    int can_split;

    can_split = seg->has_video && st->codec->codec_type == AVMEDIA_TYPE_VIDEO && (pkt->flags & AV_PKT_FLAG_KEY);

    if (seg->observe_valid_frames) {
        if (seg->next_valid_frame < seg->frame_count) {
            token = av_strtok(NULL, seg->valid_frame_delimiter, &seg->next_valid_frame_ptr);
            if (token) {
                seg->next_valid_frame = strtol(token, NULL, 10);
            }
        }
        if (seg->next_valid_frame != seg->frame_count) {
            can_split = 0;
        }
    }

    if (can_split && av_compare_ts(pkt->pts, st->time_base, end_pts, AV_TIME_BASE_Q) >= 0) {

        av_log(s, AV_LOG_DEBUG, "Next segment starts at %d %"PRId64" with frame count of %"PRId64" \n",
               pkt->stream_index, pkt->pts, seg->frame_count);

        seg->next_valid_frame = -1;

        ret = segment_end(oc);

        if (!ret)
            ret = segment_start(s);

        if (ret)
            goto fail;

        if (seg->list) {
            avio_printf(seg->pb, "%s\n", oc->filename);
            avio_flush(seg->pb);
            if (seg->size && !(seg->number % seg->size)) {
                avio_close(seg->pb);
                if ((ret = avio_open2(&seg->pb, seg->list, AVIO_FLAG_WRITE,
                                      &s->interrupt_callback, NULL)) < 0)
                    goto fail;
            }
        }
    }

    if (st->codec->codec_type == AVMEDIA_TYPE_VIDEO) {
        seg->frame_count++;
    }

    ret = oc->oformat->write_packet(oc, pkt);

fail:
    if (ret < 0) {
        oc->streams = NULL;
        oc->nb_streams = 0;
        if (seg->list)
            avio_close(seg->pb);
        avformat_free_context(oc);
    }

    return ret;
}

static int seg_write_trailer(struct AVFormatContext *s)
{
    SegmentContext *seg = s->priv_data;
    AVFormatContext *oc = seg->avf;
    int ret = segment_end(oc);
    if (seg->list)
        avio_close(seg->pb);
    oc->streams = NULL;
    oc->nb_streams = 0;
    avformat_free_context(oc);
    return ret;
}

#define OFFSET(x) offsetof(SegmentContext, x)
#define E AV_OPT_FLAG_ENCODING_PARAM
static const AVOption options[] = {
    { "segment_format",          "container format used for the segments",                              OFFSET(format),          AV_OPT_TYPE_STRING, {.str = NULL},  0, 0,       E },
    { "segment_time",            "segment length in seconds",                                           OFFSET(time),           AV_OPT_TYPE_FLOAT,   {.dbl = 2},     0, FLT_MAX, E },
    { "segment_valid_frames",    "comma separated list of frame numbers allowed to start segments",     OFFSET(valid_frames),   AV_OPT_TYPE_STRING,  {.str = NULL},  0, 0,       E },
    { "segment_list",            "output the segment list",                                             OFFSET(list),           AV_OPT_TYPE_STRING,  {.str = NULL},  0, 0,       E },
    { "segment_list_size",       "maximum number of playlist entries",                                  OFFSET(size),           AV_OPT_TYPE_INT,     {.dbl = 5},     0, INT_MAX, E },
    { "segment_wrap",            "number after which the index wraps",                                  OFFSET(wrap),           AV_OPT_TYPE_INT,     {.dbl = 0},     0, INT_MAX, E },
    { NULL },
};

static const AVClass seg_class = {
    .class_name = "segment muxer",
    .item_name  = av_default_item_name,
    .option     = options,
    .version    = LIBAVUTIL_VERSION_INT,
};


AVOutputFormat ff_segment_muxer = {
    .name           = "segment",
    .long_name      = NULL_IF_CONFIG_SMALL("segment muxer"),
    .priv_data_size = sizeof(SegmentContext),
    .flags          = AVFMT_GLOBALHEADER | AVFMT_NOFILE,
    .write_header   = seg_write_header,
    .write_packet   = seg_write_packet,
    .write_trailer  = seg_write_trailer,
    .priv_class     = &seg_class,
};
