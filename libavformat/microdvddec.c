/*
 * MicroDVD subtitle demuxer
 * Copyright (c) 2010  Aurelien Jacobs <aurel@gnuage.org>
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include "avformat.h"
#include "internal.h"
#include "libavutil/intreadwrite.h"

#define MAX_LINESIZE 2048


typedef struct {
    uint8_t lines[3][MAX_LINESIZE];
    int64_t pos[3];
    AVPacket last_pkt;
    int last_pkt_ready;
} MicroDVDContext;


static int microdvd_probe(AVProbeData *p)
{
    unsigned char c, *ptr = p->buf;
    int i;

    if (AV_RB24(ptr) == 0xEFBBBF)
        ptr += 3;  /* skip UTF-8 BOM */

    for (i=0; i<3; i++) {
        if (sscanf(ptr, "{%*d}{}%c",     &c) != 1 &&
            sscanf(ptr, "{%*d}{%*d}%c",  &c) != 1 &&
            sscanf(ptr, "{DEFAULT}{}%c", &c) != 1)
            return 0;
        ptr += strcspn(ptr, "\n") + 1;
    }
    return AVPROBE_SCORE_MAX;
}

static int microdvd_read_header(AVFormatContext *s)
{
    AVRational pts_info = (AVRational){ 2997, 125 };  /* default: 23.976 fps */
    MicroDVDContext *microdvd = s->priv_data;
    AVStream *st = avformat_new_stream(s, NULL);
    int i, frame;
    double fps;
    char c;

    if (!st)
        return AVERROR(ENOMEM);
    for (i=0; i<FF_ARRAY_ELEMS(microdvd->lines); i++) {
        microdvd->pos[i] = avio_tell(s->pb);
        ff_get_line(s->pb, microdvd->lines[i], sizeof(microdvd->lines[i]));
        if ((sscanf(microdvd->lines[i], "{%d}{}%6lf",    &frame, &fps) == 2 ||
             sscanf(microdvd->lines[i], "{%d}{%*d}%6lf", &frame, &fps) == 2)
            && frame <= 1 && fps > 3 && fps < 100)
            pts_info = av_d2q(fps, 100000);
        if (sscanf(microdvd->lines[i], "{DEFAULT}{}%c", &c) == 1) {
            st->codec->extradata = av_strdup(microdvd->lines[i] + 11);
            st->codec->extradata_size = strlen(st->codec->extradata);
            i--;
        }
    }
    avpriv_set_pts_info(st, 64, pts_info.den, pts_info.num);
    st->codec->codec_type = AVMEDIA_TYPE_SUBTITLE;
    st->codec->codec_id   = CODEC_ID_MICRODVD;
    return 0;
}

static int64_t get_pts(const char *buf)
{
    int frame;
    char c;

    if (sscanf(buf, "{%d}{%c", &frame, &c) == 2)
        return frame;
    return AV_NOPTS_VALUE;
}

static int get_duration(const char *buf)
{
    int frame_start, frame_end;

    if (sscanf(buf, "{%d}{%d}", &frame_start, &frame_end) == 2)
        return frame_end - frame_start;
    return -1;
}

static int microdvd_read_packet(AVFormatContext *s, AVPacket *pkt)
{
    MicroDVDContext *microdvd = s->priv_data;
    char buffer[MAX_LINESIZE];
    int64_t pos = avio_tell(s->pb);
    int i, len = 0, res = AVERROR_EOF;

    // last packet has its duration set but couldn't be raised earlier
    if (microdvd->last_pkt_ready) {
        *pkt = microdvd->last_pkt;
        microdvd->last_pkt_ready = 0;
        return 0;
    }

    for (i=0; i<FF_ARRAY_ELEMS(microdvd->lines); i++) {
        if (microdvd->lines[i][0]) {
            strcpy(buffer, microdvd->lines[i]);
            pos = microdvd->pos[i];
            len = strlen(buffer);
            microdvd->lines[i][0] = 0;
            break;
        }
    }
    if (!len)
        len = ff_get_line(s->pb, buffer, sizeof(buffer));

    if (microdvd->last_pkt.duration == -1 && !buffer[0]) {
        // if the previous subtitle line had no duration, last until the end of
        // the presentation
        microdvd->last_pkt.duration = 0;
        *pkt = microdvd->last_pkt;
        pkt->duration = -1;
        res = 0;
    } else if (buffer[0] && !(res = av_new_packet(pkt, len))) {
        memcpy(pkt->data, buffer, len);
        pkt->flags |= AV_PKT_FLAG_KEY;
        pkt->pos = pos;
        pkt->pts = pkt->dts = get_pts(buffer);

        if (pkt->pts != AV_NOPTS_VALUE) {
            pkt->duration = get_duration(buffer);
            if (microdvd->last_pkt.duration == -1) {
                // previous packet wasn't raised because it was lacking the
                // duration info, so set its duration with the new packet pts
                // and raise it
                AVPacket tmp_pkt;

                tmp_pkt = microdvd->last_pkt;
                tmp_pkt.duration = pkt->pts - tmp_pkt.pts;
                microdvd->last_pkt = *pkt;
                microdvd->last_pkt_ready = pkt->duration != -1;
                *pkt = tmp_pkt;
            } else if (pkt->duration == -1) {
                // no packet without duration queued, and current one is
                // lacking the duration info, we need to parse another subtitle
                // event.
                microdvd->last_pkt = *pkt;
                res = AVERROR(EAGAIN);
            }
        }
    }
    return res;
}

AVInputFormat ff_microdvd_demuxer = {
    .name           = "microdvd",
    .long_name      = NULL_IF_CONFIG_SMALL("MicroDVD subtitle format"),
    .priv_data_size = sizeof(MicroDVDContext),
    .read_probe     = microdvd_probe,
    .read_header    = microdvd_read_header,
    .read_packet    = microdvd_read_packet,
    .flags          = AVFMT_GENERIC_INDEX,
};
