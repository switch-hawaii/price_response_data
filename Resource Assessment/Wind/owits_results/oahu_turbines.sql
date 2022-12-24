\copy (select turbine_id, cluster_id, x_turbine as lat, y_turbine as lon, speed_ratio, avg_s80, turb_model, turb_class_type from turbine join cluster_turbine using (grid_id, turbine_id) order by cluster_id, turbine_id) to 'oahu_turbines.csv' with CSV header;

