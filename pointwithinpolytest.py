import ogr

# load the shape file as a layer
drv    = ogr.GetDriverByName('ESRI Shapefile')
ds_in  = drv.Open("prov_ab_p_geo83_e.shp")
lyr_in = ds_in.GetLayer(0)

# field index for which i want the data extracted 
# ("satreg2" was what i was looking for)
idx_reg = lyr_in.GetLayerDefn().GetFieldIndex("NAME")


def check(lon, lat):
  # create point geometry
  pt = ogr.Geometry(ogr.wkbPoint)
  pt.SetPoint_2D(0, lon, lat)
  lyr_in.SetSpatialFilter(pt)

  # go over all the polygons in the layer see if one include the point
  for feat_in in lyr_in:
    # roughly subsets features, instead of go over everything
    ply = feat_in.GetGeometryRef()

    # test
    if ply.Contains(pt):
      # TODO do what you need to do here
      print("point is in BC")

check(-126.5283,56.9704)