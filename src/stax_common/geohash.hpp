#pragma once

#include <cstdint>
#include <string>
#include <cmath>

namespace GeoHash {

static inline uint64_t encode(double latitude, double longitude, int precision = 64) {
    uint64_t geohash = 0;
    double lat_min = -90.0, lat_max = 90.0;
    double lon_min = -180.0, lon_max = 180.0;
    bool is_lon = true;

    for (int i = 0; i < precision; ++i) {
        geohash <<= 1;
        if (is_lon) {
            double mid = lon_min + (lon_max - lon_min) / 2.0;
            if (longitude > mid) {
                geohash |= 1;
                lon_min = mid;
            } else {
                lon_max = mid;
            }
        } else {
            double mid = lat_min + (lat_max - lat_min) / 2.0;
            if (latitude > mid) {
                geohash |= 1;
                lat_min = mid;
            } else {
                lat_max = mid;
            }
        }
        is_lon = !is_lon;
    }
    return geohash;
}

static inline std::pair<double, double> decode(uint64_t geohash, int precision = 64) {
    double lat_min = -90.0, lat_max = 90.0;
    double lon_min = -180.0, lon_max = 180.0;
    bool is_lon = true;

    for (int i = 0; i < precision; ++i) {
        uint64_t mask = 1ULL << (precision - 1 - i);
        if (is_lon) {
            double mid = lon_min + (lon_max - lon_min) / 2.0;
            if ((geohash & mask) != 0) {
                lon_min = mid;
            } else {
                lon_max = mid;
            }
        } else {
            double mid = lat_min + (lat_max - lat_min) / 2.0;
            if ((geohash & mask) != 0) {
                lat_min = mid;
            } else {
                lat_max = mid;
            }
        }
        is_lon = !is_lon;
    }

    double latitude = lat_min + (lat_max - lat_min) / 2.0;
    double longitude = lon_min + (lon_max - lon_min) / 2.0;
    return {latitude, longitude};
}

} 