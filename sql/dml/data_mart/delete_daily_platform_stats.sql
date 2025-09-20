ALTER TABLE data_mart.daily_platform_stats
    DELETE WHERE dt = %(dt)s;
