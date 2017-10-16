require 'myslog'

class Fluent::RdsMysqlLogInput < Fluent::Input
  Fluent::Plugin.register_input('rds_mysql_log', self)

  LOG_REGEXP = /^(?<time>\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2})( (?<pid>\d+))?( \[(?<message_level>[^\]]*?)\])? (?<message>.*)$/

  config_param :access_key_id, :string, :default => nil
  config_param :secret_access_key, :string, :default => nil
  config_param :region, :string, :default => nil
  config_param :db_instance_identifier, :string, :default => nil
  config_param :pos_file, :string, :default => "fluent-plugin-rds-mysql-log-pos.dat"
  config_param :refresh_interval, :integer, :default => 30
  config_param :tag, :string, :default => "rds-mysql.log"
  config_param :output_per_file, :bool, default: false
  config_param :raw_output, :bool, default: false

  def configure(conf)
    super
    require 'aws-sdk'

    raise Fluent::ConfigError.new("region is required") unless @region
    if !has_iam_role?
      raise Fluent::ConfigError.new("access_key_id is required") if @access_key_id.nil?
      raise Fluent::ConfigError.new("secret_access_key is required") if @secret_access_key.nil?
    end
    raise Fluent::ConfigError.new("db_instance_identifier is required") unless @db_instance_identifier
    raise Fluent::ConfigError.new("pos_file is required") unless @pos_file
    raise Fluent::ConfigError.new("refresh_interval is required") unless @refresh_interval
    raise Fluent::ConfigError.new("tag is required") unless @tag
  end

  def start
    super

    # pos file touch
    File.open(@pos_file, File::RDWR|File::CREAT).close

    begin
      options = {
        :region => @region,
      }
      if @access_key_id && @secret_access_key
        options[:access_key_id] = @access_key_id
        options[:secret_access_key] = @secret_access_key
      end
      @rds = Aws::RDS::Client.new(options)
    rescue => e
      $log.warn "RDS Client error occurred: #{e.message}"
    end

    @loop = Coolio::Loop.new
    timer_trigger = TimerWatcher.new(@refresh_interval, true, &method(:input))
    timer_trigger.attach(@loop)
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    super
    @loop.stop
    @thread.join
  end

  private

  def run
    @loop.run
  end

  def input
    get_and_parse_posfile
    log_files = get_logfile_list
    get_logfile(log_files)
    put_posfile
  end

  def has_iam_role?
    begin
      ec2 = Aws::EC2::Client.new(region: @region)
      !ec2.config.credentials.nil?
    rescue => e
      $log.warn "EC2 Client error occurred: #{e.message}"
    end
  end

  def get_and_parse_posfile
    begin
      # get & parse pos file
      $log.debug "pos file get start"

      pos_last_written_timestamp = 0
      pos_info = {}
      pos_log_hash = {}
      File.open(@pos_file, File::RDONLY) do |file|
        file.each_line do |line|

          pos_match = /^timestamp: (\d+)$/.match(line)
          if pos_match
            pos_last_written_timestamp = pos_match[1].to_i
            $log.debug "pos_last_written_timestamp: #{pos_last_written_timestamp}"
          end

          pos_match = /^(.+)\t(.+)\t(.+)$/.match(line)
          if pos_match
            pos_info[pos_match[1]] = pos_match[2]
            pos_log_hash[pos_match[1]] = pos_match[3]
            $log.debug "log_file: #{pos_match[1]}, marker: #{pos_match[2]}, hash: #{pos_match[3]}"
          end
        end
        @pos_last_written_timestamp = pos_last_written_timestamp
        @pos_info = pos_info
        @pos_log_hash = pos_log_hash
      end
    rescue => e
      $log.warn "pos file get and parse error occurred: #{e.message}"
    end
  end

  def put_posfile
    # pos file write
    @pos_log_hash = @current_pos_log_hash
    begin
      $log.debug "pos file write"
      File.open(@pos_file, File::WRONLY|File::TRUNC) do |file|
        file.puts "timestamp: #{@pos_last_written_timestamp.to_s}"

        @pos_info.each do |log_file_name, marker|
          file.puts "#{log_file_name}\t#{marker}\t#{@pos_log_hash[log_file_name]}"
        end
      end
    rescue => e
      $log.warn "pos file write error occurred: #{e.message}"
    end
  end

  def get_logfile_list
    begin
      $log.debug "get logfile-list from rds: db_instance_identifier=#{@db_instance_identifier}, pos_last_written_timestamp=#{@pos_last_written_timestamp}"
      log_files = @rds.describe_db_log_files(
        db_instance_identifier: @db_instance_identifier,
        file_last_written: @pos_last_written_timestamp,
        max_records: 10,
      )

      @current_pos_log_hash = {}
      log_files.each do |log_file|
        log_file.describe_db_log_files.each do |item|
          log_file_name = item[:log_file_name]
          @current_pos_log_hash[log_file_name] = item.hash
        end
      end

      log_files
    rescue => e
      $log.warn "RDS Client describe_db_log_files error occurred: #{e.message}"
    end
  end

  def rotated?(log_file)
      utc_hour = (Time.now - 3600).utc.hour
      hash_target = ""
      case log_file
      when "error/mysql-error.log" then
        hash_target = "error/mysql-error-running.log"
      else
        hash_target = "#{log_file}.#{utc_hour}"
      end
      return @current_pos_log_hash[hash_target] && @pos_log_hash[hash_target] != @current_pos_log_hash[hash_target]
  end

  def get_logfile(log_files)
    begin
      log_files.each do |log_file|
        log_file.describe_db_log_files.each do |item|
          # save maximum written timestamp value
          @pos_last_written_timestamp = item[:last_written] if @pos_last_written_timestamp < item[:last_written]

          # log file download
          log_file_name = item[:log_file_name]
          next if log_file_name[%r{error/mysql-error-running.log$}]
            || log_file_name[%r{error/mysql-error.log-old$}]
            || log_file_name[%r{slowquery/mysql-slowquery.log\.}]
            || log_file_name[%r{general/mysql-general.log\.}]
          marker = @pos_info.has_key?(log_file_name) ? @pos_info[log_file_name] : "0"
          marker = "0" if rotated?(log_file_name)

          $log.debug "download log from rds: log_file_name=#{log_file_name}, marker=#{marker}"
          logs = @rds.download_db_log_file_portion(
            db_instance_identifier: @db_instance_identifier,
            log_file_name: log_file_name,
            marker: marker,
          )
          raw_records = get_logdata(logs)

          #emit
          parse_and_emit(raw_records, log_file_name) unless raw_records.nil?
        end
      end
    rescue => e
      $log.warn e.message
    end
  end

  def get_logdata(logs)
    log_file_name = logs.context.params[:log_file_name]
    raw_records = []
    begin
      logs.each do |log|
        # save got line's marker
        @pos_info[log_file_name] = log.marker

        raw_records += log.log_file_data.split("\n")
      end
    rescue => e
      $log.warn e.message
    end
    return raw_records
  end

  def divide_slow_log(lines)
    records = []
    line = lines.shift
    while line
      record = []
      while line != nil && line.start_with?("#")
        record << line.strip
        line = lines.shift
      end
      while line != nil && !line.start_with?("#")
        record << line.strip
        line = lines.shift
      end
      records << record
    end
    records
  end

  def parse_and_emit(raw_records, log_file_name)
    begin
      $log.debug "raw_records.count: #{raw_records.count}"

      record = {
        "db_instance_identifier" => @db_instance_identifier,
        "region" => @region,
        "log_file_name" => log_file_name,
      }
      output_tag = @tag
      output_tag += ".#{log_file_name.gsub(/\/|\./, '_')}" if @output_per_file

      if log_file_name != "slowquery/mysql-slowquery.log"
        raw_records.each do |raw_record|
          $log.debug "raw_record=#{raw_record}"
          line_match = LOG_REGEXP.match(raw_record)
          next unless line_match

          record["raw"] = raw_record if @raw_output
          record["time"] = line_match[:time]
          record["message"] = line_match[:message]
          record["pid"] = line_match[:pid] if line_match[:pid]
          record["message_level"] = line_match[:message_level] if line_match[:message_level]

          router.emit(output_tag, Time.parse(line_match[:time] + ' +0000').to_i, record)
        end
      else
        divide_slow_log(raw_records).each do |raw_record|
          $log.debug "raw_record=#{raw_record}"
          begin
            raw = raw_record.join("\n") if @raw_output
            record = record.merge(stringify_keys(MySlog.new.parse_record(raw_record)))
            record["raw"] = raw if @raw_output
            if time = record.delete('date')
              time = time.to_i
            else
              time = Time.now.to_i
            end

            router.emit(output_tag, time, record)
          rescue => e
            $log.warn e.message
          end
        end
      end
    rescue => e
      $log.warn e.message
    end
  end

  def stringify_keys(record)
    result = {}
    record.each_key do |key|
      result[key.to_s] = record[key]
    end
    result
  end

  class TimerWatcher < Coolio::TimerWatcher
    def initialize(interval, repeat, &callback)
      @callback = callback
      on_timer # first call
      super(interval, repeat)
    end

    def on_timer
      @callback.call
    end
  end
end
