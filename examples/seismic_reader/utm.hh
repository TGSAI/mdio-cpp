// Copyright 2025 TGS

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "mdio/mdio.h"
#include <tuple>
#include <cmath>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <type_traits>

namespace utm {

/**
 * @brief Base class for coordinate pairs
 */
struct CoordPair {
    double first;
    double second;
    
    CoordPair(double a = 0.0, double b = 0.0) : first(a), second(b) {}
};

/**
 * @brief Geographic coordinates (latitude, longitude) in decimal degrees
 */
struct GeoCoord : public CoordPair {
    GeoCoord(double lat = 0.0, double lon = 0.0) : CoordPair(lat, lon) {}
    
    double& latitude = first;
    double& longitude = second;
};

/**
 * @brief UTM coordinates (easting, northing) in meters
 */
struct UTMCoord : public CoordPair {
    UTMCoord(double e = 0.0, double n = 0.0) : CoordPair(e, n) {}
    
    double& easting = first;
    double& northing = second;
};

/**
 * @brief Represents the extent (min, max) of coordinates
 */
struct CoordExtent : public CoordPair {
    CoordExtent(double mn = 0.0, double mx = 0.0) : CoordPair(mn, mx) {}
    
    double& min = first;
    double& max = second;

    /**
     * @brief Check if this extent represents a single point (min equals max)
     * @return true if min equals max, false otherwise
     */
    bool is_point() const {
        return min == max;
    }
};

/**
 * @brief Convert UTM coordinates (easting, northing) to geographic coordinates (latitude, longitude)
 * for GDA94 / MGA Zone 51
 * 
 * @param utm UTM coordinates in meters
 * @return GeoCoord Geographic coordinates in decimal degrees
 */
inline GeoCoord utm_to_geo(const UTMCoord& utm) {
    // Constants for GDA94 / MGA Zone 51 (EPSG:28351)
    const double a = 6378137.0;           // Semi-major axis
    const double f = 1.0 / 298.257222101; // Flattening
    const double k0 = 0.9996;             // Scale factor
    const double central_meridian = 123.0; // Central meridian for Zone 51
    const double false_easting = 500000.0; // False easting
    const double false_northing = 10000000.0; // False northing (Southern Hemisphere)
    
    // Calculate ellipsoid parameters
    const double e2 = 2 * f - f * f;      // Eccentricity squared
    const double e_prime_squared = e2 / (1 - e2);
    
    // Adjust for false easting and northing
    const double x = utm.easting - false_easting;
    const double y = utm.northing - false_northing;
    
    // Meridian distance
    const double m = y / k0;
    
    // Footprint latitude
    const double mu = m / (a * (1 - e2/4 - 3*e2*e2/64 - 5*e2*e2*e2/256));
    
    // Coefficients for footprint latitude
    const double e1 = (1 - sqrt(1 - e2)) / (1 + sqrt(1 - e2));
    const double j1 = 3*e1/2 - 27*e1*e1*e1/32;
    const double j2 = 21*e1*e1/16 - 55*e1*e1*e1*e1/32;
    const double j3 = 151*e1*e1*e1/96;
    const double j4 = 1097*e1*e1*e1*e1/512;
    
    // Footprint latitude
    const double fp = mu + j1*sin(2*mu) + j2*sin(4*mu) + j3*sin(6*mu) + j4*sin(8*mu);
    
    // Calculate parameters
    const double cos_fp = cos(fp);
    const double sin_fp = sin(fp);
    const double tan_fp = tan(fp);
    
    const double C1 = e_prime_squared * cos_fp * cos_fp;
    const double T1 = tan_fp * tan_fp;
    const double R1 = a * (1 - e2) / pow(1 - e2 * sin_fp * sin_fp, 1.5);
    const double N1 = a / sqrt(1 - e2 * sin_fp * sin_fp);
    
    const double D = x / (N1 * k0);
    
    // Calculate latitude
    const double lat_rad = fp - (tan_fp / (R1 * N1)) * (
        D*D/2 - 
        (5 + 3*T1 + 10*C1 - 4*C1*C1 - 9*e_prime_squared) * D*D*D*D/24 + 
        (61 + 90*T1 + 298*C1 + 45*T1*T1 - 252*e_prime_squared - 3*C1*C1) * D*D*D*D*D*D/720
    );
    
    // Calculate longitude
    const double lon_rad = central_meridian * M_PI / 180.0 + (
        D - 
        (1 + 2*T1 + C1) * D*D*D/6 + 
        (5 - 2*C1 + 28*T1 - 3*C1*C1 + 8*e_prime_squared + 24*T1*T1) * D*D*D*D*D/120
    ) / cos_fp;
    
    // Convert to degrees
    GeoCoord result;
    result.latitude = lat_rad * 180.0 / M_PI;
    result.longitude = lon_rad * 180.0 / M_PI;
    
    return result;
}

namespace geojson {

/**
 * @brief Encode a string in URL format.
 * @param value The string to encode.
 * @return The encoded string.
 */
std::string urlEncode(const std::string &value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (unsigned char c : value) {
        // Encode reserved characters
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::uppercase << std::setw(2) << int(c);
        }
    }
    return escaped.str();
}

/**
 * @brief Encode a point in GeoJSON format and return a URL to a web map.
 * @param geo Geographic coordinates (latitude, longitude)
 * @return A URL to a web map.
 */
std::string encodeGeoJSONPointURL(const GeoCoord& geo) {
    std::ostringstream geojson;
    geojson << R"({"type":"Point","coordinates":[)"
            << geo.longitude << "," << geo.latitude << "]}";

    std::string geojsonStr = geojson.str();
    std::string encodedGeoJSON = urlEncode(geojsonStr);

    return "http://geojson.io/#data=data:application/json," + encodedGeoJSON;
}

/**
 * @brief Encode a bounding box in GeoJSON format and return a URL to a web map.
 * @param x_extents The extents of the coordinates in the x direction.
 * @param y_extents The extents of the coordinates in the y direction.
 * @return A URL to a web map.
 */
std::string encodeGeoJSONBoundingBoxURL(const CoordExtent& x_extents, const CoordExtent& y_extents) {
    // Calculate the four corners in UTM coordinates
    UTMCoord nw = {x_extents.min, y_extents.max};
    UTMCoord ne = {x_extents.max, y_extents.max};
    UTMCoord sw = {x_extents.min, y_extents.min};
    UTMCoord se = {x_extents.max, y_extents.min};
    
    // Convert to geographic coordinates
    GeoCoord nw_geo = utm_to_geo(nw);
    GeoCoord ne_geo = utm_to_geo(ne);
    GeoCoord sw_geo = utm_to_geo(sw);
    GeoCoord se_geo = utm_to_geo(se);

    std::ostringstream geojson;
    geojson << R"({"type":"Polygon","coordinates":[[)"
            << "[" << nw_geo.longitude << "," << nw_geo.latitude << "],"
            << "[" << ne_geo.longitude << "," << ne_geo.latitude << "],"
            << "[" << se_geo.longitude << "," << se_geo.latitude << "],"
            << "[" << sw_geo.longitude << "," << sw_geo.latitude << "],"
            << "[" << nw_geo.longitude << "," << nw_geo.latitude << "]]]}";  // Close the polygon

    std::string geojsonStr = geojson.str();
    std::string encodedGeoJSON = urlEncode(geojsonStr);

    return "http://geojson.io/#data=data:application/json," + encodedGeoJSON;
}

}  // namespace geojson

/**
 * @brief Print the four corners of the geometry in UTM coordinates and their corresponding geographic coordinates.
 * @param x_extents The extents of the coordinates in the x direction.
 * @param y_extents The extents of the coordinates in the y direction.
 */
void print_corners(const CoordExtent& x_extents, const CoordExtent& y_extents) {
    // Display the four corners of the geometry
    std::cout << "\nGeometry Corners (UTM -> Lat/Long):" << std::endl;
    std::cout << "-------------------------------------" << std::endl;
    
    // Check if we're dealing with a point (both extents are points with the same value)
    if (x_extents.is_point() && y_extents.is_point()) {
        GeoCoord geo = utm_to_geo({x_extents.min, y_extents.min});
        std::cout << "Point: E=" << x_extents.min << ", N=" << y_extents.min
                  << " -> Lat=" << geo.latitude << "°, Lon=" << geo.longitude << "°" << std::endl;
        return;
    }

    // Northwest corner
    UTMCoord nw = {x_extents.min, y_extents.max};
    GeoCoord nw_geo = utm_to_geo(nw);
    std::cout << "Northwest: E=" << nw.easting << ", N=" << nw.northing
              << " -> Lat=" << nw_geo.latitude << "°, Lon=" << nw_geo.longitude << "°" << std::endl;

    // Northeast corner
    UTMCoord ne = {x_extents.max, y_extents.max};
    GeoCoord ne_geo = utm_to_geo(ne);
    std::cout << "Northeast: E=" << ne.easting << ", N=" << ne.northing
              << " -> Lat=" << ne_geo.latitude << "°, Lon=" << ne_geo.longitude << "°" << std::endl;

    // Southwest corner
    UTMCoord sw = {x_extents.min, y_extents.min};
    GeoCoord sw_geo = utm_to_geo(sw);
    std::cout << "Southwest: E=" << sw.easting << ", N=" << sw.northing
              << " -> Lat=" << sw_geo.latitude << "°, Lon=" << sw_geo.longitude << "°" << std::endl;

    // Southeast corner
    UTMCoord se = {x_extents.max, y_extents.min};
    GeoCoord se_geo = utm_to_geo(se);
    std::cout << "Southeast: E=" << se.easting << ", N=" << se.northing
              << " -> Lat=" << se_geo.latitude << "°, Lon=" << se_geo.longitude << "°" << std::endl;
}

/**
 * @brief Present a link to a web map with either the maximum area of the available extents or a single point of interest.
 * @param x_extents The extents of the coordinates in the x direction.
 * @param y_extents The extents of the coordinates in the y direction.
 */
void web_display(const CoordExtent& x_extents, const CoordExtent& y_extents) {
    // Calculate the four corners in UTM coordinates
    UTMCoord nw = {x_extents.min, y_extents.max};
    UTMCoord ne = {x_extents.max, y_extents.max};
    UTMCoord sw = {x_extents.min, y_extents.min};
    UTMCoord se = {x_extents.max, y_extents.min};
    
    // Convert to geographic coordinates
    GeoCoord nw_geo = utm_to_geo(nw);
    GeoCoord ne_geo = utm_to_geo(ne);
    GeoCoord sw_geo = utm_to_geo(sw);
    GeoCoord se_geo = utm_to_geo(se);
    
    // Check if we're dealing with a point
    if (x_extents.is_point() && y_extents.is_point()) {
        std::cout << "Click here to view the point on a global map: " 
                  << geojson::encodeGeoJSONPointURL(nw_geo) << std::endl;
    } else {
        std::cout << "Click here to view the area on a global map: " 
                  << geojson::encodeGeoJSONBoundingBoxURL(x_extents, y_extents) << std::endl;
    }
}

/**
 * @brief Backward compatibility version of print_corners that accepts tuples
 * @param cdp_x_extents The extents of the CDP coordinates in the x direction as a tuple.
 * @param cdp_y_extents The extents of the CDP coordinates in the y direction as a tuple.
 */
void print_corners(std::tuple<mdio::dtypes::float64_t, mdio::dtypes::float64_t> cdp_x_extents,
                  std::tuple<mdio::dtypes::float64_t, mdio::dtypes::float64_t> cdp_y_extents) {
    CoordExtent x_ext = {std::get<0>(cdp_x_extents), std::get<1>(cdp_x_extents)};
    CoordExtent y_ext = {std::get<0>(cdp_y_extents), std::get<1>(cdp_y_extents)};
    print_corners(x_ext, y_ext);
}

/**
 * @brief Backward compatibility version of web_display that accepts tuples
 * @param cdp_x_extents The extents of the CDP coordinates in the x direction as a tuple.
 * @param cdp_y_extents The extents of the CDP coordinates in the y direction as a tuple.
 */
void web_display(std::tuple<mdio::dtypes::float64_t, mdio::dtypes::float64_t> cdp_x_extents,
                std::tuple<mdio::dtypes::float64_t, mdio::dtypes::float64_t> cdp_y_extents) {
    CoordExtent x_ext = {std::get<0>(cdp_x_extents), std::get<1>(cdp_x_extents)};
    CoordExtent y_ext = {std::get<0>(cdp_y_extents), std::get<1>(cdp_y_extents)};
    web_display(x_ext, y_ext);
}

} // namespace utm
