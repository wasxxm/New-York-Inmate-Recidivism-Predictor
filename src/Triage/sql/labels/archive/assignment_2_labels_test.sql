-- start with a comment
select joid as entity_id, 1 as outcome
  from cleaned.jocojimsinmatedata
  -- just a comment
 where booking_date_full between date('{as_of_date}') and date('{as_of_date}') + interval '{label_timespan}'
       and coalesce(release_date_full, '2099-01-01 00:00:00') - booking_date_full > '2 weeks'::interval
 group by joid