# Amazon RDS for MySQL log input plugin for fluentd

## Overview
- Amazon Web Services RDS log input plugin for fluentd

## Configuration

```config
<source>
  type rds_mysql_log
  # required
  region                 <region name>
  db_instance_identifier <instance identifier>
  # optional if you can IAM credentials
  access_key_id          <access_key>
  secret_access_key      <secret_access_key>
  # optional
  refresh_interval       <interval number by second(default: 30)>
  tag                    <tag name(default: rds-mysql.log>
  pos_file               <log getting position file(default: rds-mysql.log)>
</source>
```

### Example setting
```config
<source>
  type rds_mysql_log
  region ap-northeast-1
  db_instance_identifier test-mysql
  access_key_id     XXXXXXXXXXXXXXXXXXXX
  secret_access_key xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  refresh_interval  30
  tag mysql.log
  pos_file /tmp/mysql-log-pos.dat
</source>

<match mysql.log>
  type stdout
</match>
```
