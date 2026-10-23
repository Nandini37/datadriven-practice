select distinct feat_name from ml_features 
where source = 'user_sessions'
  and avg_val is null
order by feat_name;
