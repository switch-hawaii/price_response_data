#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import math, os, sys, time, urllib, itertools
import osr, gdal
import numpy as np
from PIL import Image, ImageFilter, ImageOps, ImageChops
import pyproj  # see http://gis.stackexchange.com/a/78944/75972

# bounding box to download maps for (full mosaics will cover this)
# Wahiawa, near center of Oahu
# lat_min = 21.509
# lon_min = -158.027
# lat_max = 21.509
# lon_max = -158.027
# Manoa
# lat_min = 21.308033
# lon_min = -157.808206
# lat_max = 21.308033
# lon_max = -157.808206
# # bigger test
# lat_min = 21.308033
# lon_min = -158.027
# lat_max = 21.509
# lon_max = -157.808206
# all Oahu (boundaries of NSRDB grid over Oahu)
lat_min = 21.23
lon_min = -158.32
lat_max = 21.75
lon_max = -157.60

# list of roof colors (RGBA); everything else is considered background
# This is probably too long to do exhaustively, and it will have trouble
# with 3D buildings with peaked roofs, e.g., 21.297561, -157.817724
# or complex roofs, e.g., 21.300070, -157.833817
roof_colors = {
    # gray on transparent background
    (242, 242, 242, 242),
    # gray on colored background (e.g., military base)
    (242, 242, 242, 255),
    (240, 240, 240, 255), # multi-level roof? e.g., 21.325898, -157.849181
    (238, 238, 238, 255), # multi-level roof? e.g., 21.327981, -157.844793
    # gray buildings in commercial areas (mostly commercial)
    (249, 237, 214, 255), # near 21.497264, -158.025791
    (250, 236, 211, 255),
    # commercial buildings on commercial background
    (249, 237, 213, 255), # near 21.497264, -158.025791
    # commercial buildings with internal maps?
    (242, 234, 214, 255), # e.g., 21.496347, -158.025577
    # commercial building on transparent background
    (251, 238, 210, 242), # e.g., 21.533427, -158.023860
    # 3D commercial building on gray background?
    (247, 237, 218, 255), # 21.307942, -157.836724
    (246, 237, 218, 255),
    (248, 239, 220, 255),
    (245, 239, 216, 255),
    (247, 240, 216, 255),

    # commercial building on gray background
    (247, 238, 214, 255), # e.g., 21.510401, -158.021671
    (247, 237, 213, 255), # e.g., 21.327697, -157.848601
    (251, 237, 214, 255), # e.g., 21.323039, -157.849191
    (247, 237, 214, 255), # e.g., 21.324573, -157.843258
    (248, 237, 214, 255),
}
# list of background colors (RGBA); everything else is considered rooftop
background_colors = {
    (0, 0, 0, 0),               # standard
    (234, 234, 234, 255),       # military bases? and residential areas
    # commercial areas (parking lots?) at zoom 17; may change to gray at zoom 18
    (238, 230, 219, 255),
    (237, 230, 218, 255),    # 21.4998,-158.0251; 21.38907,-157.95399; 21.37907,-157.93081
    (241, 223, 198, 255),
    (242, 224, 197, 255),

    # road direction arrows
    (186, 182, 190, 255), (182, 186, 202, 255),
    (181, 185, 201, 255), (183, 187, 203, 255),
    (184, 184, 192, 255),
    (210, 210, 218, 255), # ? is this real? where is it?
    (246, 246, 246, 28),  # partially transparent off-ramp, 21.48915, -158.02692
    (242, 242, 242, 212),  # on-road arrows at 21.3517, -157.9317 (note: this is also a standard color for roofs, but different alpha)
}

# note: some roofs are in google satellite images but missing from the map tiles:
# 21.5034,-158.0122; 21.44511,-157.96106; 21.36900,-157.90504; 21.49678,-157.85153;
# 21.60510,-157.90255; 21.47494,-158.21599; 21.56687,-158.11839

# some roofs are bigger in the map tiles than in the satellite image (rare):
# 21.5230,-158.0119; 21.30839,-157.83727 (maybe parking on roof?)

# TODO: check the white strip along edge of mosaic 21.4326168645_-158.181152344.tif

# this comes from https://developers.google.com/maps/documentation/static-maps/
with open('google_api_key.txt') as f:
    google_api_key = f.read().strip()

zoom_level = 17         # google maps zoom level
pixels_per_tile = 512   # size of each tile in x and y direction
tiles_per_mosaic = 8    # number of tiles to aggregate in x and y direction

# calculate geography
web_mercator_proj = pyproj.Proj(init='epsg:3857')
geographic_proj = pyproj.Proj(init='epsg:4326')

pixels_in_world = 2**zoom_level * 256    # whole world is 256 pixels at zoom level 0

# x distance between 180 deg E/W
world_size_meters = (
    pyproj.transform(geographic_proj, web_mercator_proj, 180, 0)[0]
    - pyproj.transform(geographic_proj, web_mercator_proj, -180, 0)[0]
)

meters_per_pixel = world_size_meters / pixels_in_world
meters_per_tile = pixels_per_tile * meters_per_pixel
meters_per_mosaic = meters_per_tile * tiles_per_mosaic

# bounding box coordinates in meters, using Web Mercator projection
x_min, y_min = pyproj.transform(geographic_proj, web_mercator_proj, lon_min, lat_min)
x_max, y_max = pyproj.transform(geographic_proj, web_mercator_proj, lon_max, lat_max)

# mosaic index corresponding to each corner
mx_min = int(x_min // meters_per_mosaic)
my_min = int(y_min // meters_per_mosaic)
mx_max = int(x_max // meters_per_mosaic)
my_max = int(y_max // meters_per_mosaic)

# setup output directories
for path in ['raw', 'roof_tiffs', 'orig_tiffs']:
    if not os.path.exists(path):
        os.makedirs(path)

# yield elements in the range of (x_min, y_min) to (x_max, y_max) (inclusive)
def iter_xy((x_min, y_min), (x_max, y_max)):
    return itertools.product(
        xrange(int(x_min), int(x_max) + 1),
        xrange(int(y_min), int(y_max) + 1)
)

def main():
    n_done = 0
    n_todo = (mx_max-mx_min+1)*(my_max-my_min+1)*tiles_per_mosaic*tiles_per_mosaic

    bottom_buffer = 24        # number of extra pixels to retrieve and trim from top and bottom of tile
    edge_buffer = 3           # number of extra pixels to use for image processing (e.g. removing small parts)

    for mx, my in iter_xy((mx_min, my_min), (mx_max, my_max)):
        tx_min = mx * tiles_per_mosaic
        ty_min = my * tiles_per_mosaic
        tx_max = tx_min + tiles_per_mosaic - 1
        ty_max = ty_min + tiles_per_mosaic - 1
        # location of mosaic center
        m_lon, m_lat = pyproj.transform(
            web_mercator_proj, geographic_proj,
            (mx+0.5)*meters_per_mosaic, (my+0.5)*meters_per_mosaic
        )
        # create empty mosaic
        # In principle this could be 1-bit, but then we end up converting repeatedly to grayscale
        # (e.g., gdal needs a grayscale image even to make a 1-bit geotiff)
        mosaic = Image.new(mode='L', size=(pixels_per_tile * tiles_per_mosaic, pixels_per_tile * tiles_per_mosaic), color=0)
        mosaic_orig = Image.new(mode='RGBA', size=mosaic.size, color=(255, 255, 255, 0))

        # iterate over tiles within this mosaic
        for tx, ty in iter_xy((tx_min, ty_min), (tx_max, ty_max)):
            n_done += 1
            print "processing image {} of {}...".format(n_done, n_todo)

            # location of tile center
            t_lon, t_lat = pyproj.transform(
                web_mercator_proj, geographic_proj,
                (tx+0.5)*meters_per_tile, (ty+0.5)*meters_per_tile
            )

            # define the names of files to save
            raw_file = os.path.join('raw', "{}_{}.png".format(t_lat, t_lon))
            roof_file = os.path.join('roof_tiffs', "{}_{}.tif".format(m_lat, m_lon))
            orig_file = os.path.join('orig_tiffs', "{}_{}.tif".format(m_lat, m_lon))
            # png_file = os.path.join('pgw', "{}_{}.png".format(m_lat, m_lon))
            # pgw_file = png_file[:-4] + '.pgw'

            # download tile image (with a buffer to trim from top and bottom,
            # plus 1 extra pixel all around for the kernel later)
            # e.g., http://maps.googleapis.com/maps/api/staticmap?center=21.5092962002,-158.02734375&zoom=17&size=514x562&sensor=true&visual_refresh=true&style=feature:all|element:all|visibility:off&style=feature:landscape.man_made|element:geometry.fill|visibility:on
            if not os.path.exists(raw_file):
                query = (
                    "http://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}"
                    + "&zoom=17&size={w}x{h}&sensor=true&visual_refresh=true"
                    + "&style=feature:all|element:all|visibility:off"
                    + "&style=feature:landscape.man_made|element:geometry.fill|visibility:on"
                    + "&key={key}"
                ).format(
                    lat=t_lat, lon=t_lon,
                    w=pixels_per_tile+2*edge_buffer,
                    h=pixels_per_tile+2*edge_buffer+2*bottom_buffer,
                    key=google_api_key
                )
                urllib.urlretrieve(query, raw_file)

            # process raw tile files and add to the mosaic
            im = Image.open(raw_file)
            # convert from indexed color to full RGBA to allow access to pixel colors
            im = im.convert('RGBA')
            # trim buffer zone from top and bottom edges
            im = im.crop((0, bottom_buffer, im.size[0], im.size[1]-bottom_buffer))   # (left, upper, right, lower)

            # create a grayscale image showing likely roof areas in white on a black background
            bw = Image.new(mode='L', size=im.size, color=0)
            bw_data = bw.load()
            im_data = im.load()
            for x in xrange(im.size[0]):
                for y in xrange(im.size[1]):
                    # There's a short list of real background colors, which usually come through
                    # consistently because they are 100% opaque and big enough to dominate the palette.
                    # But then there are also lots of different colors for arrows. Unlike roofs, these
                    # all have red or alpha values <= 215.
                    # (If this doesn't catch them all, another option would be to change remove_small_parts()
                    # to remove everything smaller than 4 pixels across (about 5 meters), which should
                    # remove all the arrows without sacrificing usable roofs. Or the images could be downloaded
                    # with zoom level 18, which makes the roofs twice as big but not the arrows. The maps
                    # could be kept at that resolution or the images could be downsampled to the next level up.)
                    r, g, b, a = im_data[x, y]
                    if (
                        (r, g, b, a) in background_colors
                        or (r <= 215 or a <= 215)  # covers most road arrows
                        # the next two distinguish orange-tinted commercial areas from similarly colored buildings
                        or (r <= 245 and g <= 230)
                        or (r <= 240 and g <= 235)
                    ):
                        pass    # it's a background color
                    else:
                        bw_data[x, y] = 255

            # # use a kernel and threshold to filter out stray pixels (e.g., anti-aliasing along the edge of military bases)
            # # a pixel will be white if it is currently white and has at least 3 other white pixels around it (kernel >= 1.3)
            # # or if it is black but has at least 6 white pixels around it (0.6 <= kernel <= 0.8)
            # # or it will remain black only if it is black and has at least 3 other black pixels around it
            # for i in range(3):
            #     bw = bw.filter(ImageFilter.Kernel((3,3), [0.1, 0.1, 0.1, 0.1, 1.0, 0.1, 0.1, 0.1, 0.1], scale=2.0))
            #     bw = bw.point(lambda p: 255 if p*2.0/255 >= 1.29 or (0.59 <= p*2.0/255 <= 0.81) else 0)

            # filter out foreground or background regions narrower than 2 pixels across
            # these are generally anti-aliasing artifacts
            # bw = remove_small_parts(bw, background=True)
            bw = remove_small_parts(bw)
            # bw = remove_small_parts(bw)   # do this twice, to get small blips left the first time

            # crop tiles to final size
            im = im.crop((edge_buffer, edge_buffer, edge_buffer+pixels_per_tile, edge_buffer+pixels_per_tile))
            bw = bw.crop((edge_buffer, edge_buffer, edge_buffer+pixels_per_tile, edge_buffer+pixels_per_tile))

            # paste tile into mosaic(s); note, PIL counts from top/left, but tile indexes count from bottom/left
            left = (tx-tx_min)*pixels_per_tile
            top = (ty_max-ty)*pixels_per_tile
            mosaic.paste(bw, box=(left, top))

            mosaic_orig.paste(im, box=(left, top))
            # black_roofs = ImageOps.invert(bw)
            # # mosaic_orig.paste(black_roofs, box=(left, top), mask=bw)
            # blue_mat = Image.new('RGB', size=bw.size, color=(0, 0, 0))
            # mosaic_orig.paste(blue_mat, box=(left, top), mask=black_roofs.point(lambda p: 0.8*p))

        save_geotiff(mosaic, roof_file, mx, my, one_bit=True)
        save_geotiff(mosaic_orig, orig_file, mx, my)

srs = osr.SpatialReference()
# next two lines don't work because gdal package also needs to have
# GDAL_DATA enviro variable pointing to a directory with EPSG support file gcs.csv
# (which may come with conda libgdal package, see https://github.com/ContinuumIO/anaconda-issues/issues/221,
# but that requires an earlier version of xc, which previously caused problems with gdal itself)
# It may be possible to fix this by pointing GDAL_DATA to
# ~/anaconda/share/epsg_csv/ or ~/anaconda/share/gdal/
# (and then gdal could also be used to do the projection calculations, eliminating need for pyproj)
# srs.ImportFromEPSG(3857)         # Web Mercator
# web_mercator_wkt = srs.ExportToWkt()
# ogc wkt from http://spatialreference.org/ref/sr-org/6864/ (for EPSG 3857)
web_mercator_wkt = """PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["Popular Visualisation CRS",DATUM["Popular_Visualisation_Datum",SPHEROID["Popular Visualisation Sphere",6378137,0,AUTHORITY["EPSG","7059"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6055"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4055"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],AUTHORITY["EPSG","3785"],AXIS["X",EAST],AXIS["Y",NORTH]]"""
geotiff_driver = gdal.GetDriverByName('GTiff')

def remove_small_parts(image, background=False):
    """Remove areas of img that are smaller than 2 pixels across.
    img should be grayscale, with values of only 0 (background) and 255 (foreground).
    If background=True, this will remove small background regions instead of foreground."""
    grow_filter_1 = ImageFilter.Kernel((3,3), [1.0]*9, scale=1.0)
    grow_filter_2 = ImageFilter.Kernel((5,5), [1.0]*25, scale=1.0)
    invert = ImageOps.invert

    if background:
        img = invert(image)
        inv_img = image
    else:
        img = image
        inv_img = invert(image)

    # grow the background by 1 pixel (shrink the foreground)
    shrunk = invert(inv_img.filter(grow_filter_1))
    # grow the foreground by 2 pixels (will restore any rotated corners that were trimmed)
    grown = shrunk.filter(grow_filter_2)
    # small objects have been removed but foreground may be a little bigger than original image
    img = ImageChops.multiply(img, grown)

    if background:
        return invert(img)
    else:
        return img


def save_geotiff(mosaic, tif_file, mx, my, one_bit=False):
    # mosaic should be 1-bit or have 8 bits per band
    # if it's grayscale and one_bit is True, it will be saved as a one-bit image

    if mosaic.mode == '1':
        # perversely, WriteArray() doesn't work with a (boolean) array made from a 1-bit image
        mosaic = mosaic.convert('L')
        one_bit=True
        n_bands = 1

    if one_bit:
        # extra options from http://www.gdal.org/gdal_tutorial.html and http://www.gdal.org/frmt_gtiff.html
        options = ['NBITS=1', 'COMPRESS=CCITTFAX4']   # gives the best compression by a factor of 2
    else:
        options = ['COMPRESS=DEFLATE']

    # split into separate bands, so we know how many to create and can write each one individually
    bands = mosaic.split()

    dataset = geotiff_driver.Create(
        tif_file,
        mosaic.size[0],   # x pixels
        mosaic.size[1],   # y pixels
        len(bands),       # number of bands
        # data type; http://www.gdal.org/frmt_gtiff.html says use Byte to hold 1-bit data (as raw bytes)
        gdal.GDT_Byte,
        options
    )

    # see https://en.wikipedia.org/wiki/World_file for details on parameters
    geotransform = (
        # x value at center of upper left pixel
        mx * meters_per_mosaic + 0.5 * meters_per_pixel,
        # pixel size in x dir in map units
        meters_per_pixel,
        # x-skew
        0,
        # y value at center of upper left pixel (actually lower left pixel because of negative scaling)
        (my + 1) * meters_per_mosaic - 0.5 * meters_per_pixel,
        # y-skew
        0,
        # pixel size in y dir in map units (usually negative)
        -meters_per_pixel
    )

    dataset.SetGeoTransform(geotransform)
    dataset.SetProjection(web_mercator_wkt)
    for i, band in enumerate(bands):
        dataset.GetRasterBand(i+1).WriteArray(np.asarray(band))
    dataset.FlushCache()  # write to disk
    del dataset

if __name__ == '__main__':
    main()
