require 'util/mount/miq_generic_mount_session'

class MiqSwiftStorage < MiqObjectStorage
  attr_reader :container_name

  def self.uri_scheme
    "swift".freeze
  end

  def self.new_with_opts(opts)
    new(opts.slice(:uri, :username, :password))
  end

  def initialize(log_settings)
    super(log_settings)
    raise "username and password are required values!" if @settings[:username].nil? || @settings[:password].nil?
    _scheme, _userinfo, @host, @port, _registry, @mount_path, _opaque, query, _fragment = URI.split(URI.encode(@settings[:uri]))
    query_params(query)
    @username       = @settings[:username]
    @password       = @settings[:password]
    @container_name = @mount_path[0] == File::Separator ? @mount_path[1..-1] : @mount_path
  end

  def uri_to_local_path(remote_file)
    # Strip off the leading "swift:/" from the URI"
    File.join(@mnt_point, URI(remote_file).host, URI(remote_file).path)
  end

  def uri_to_object_path(remote_file)
    # Strip off the leading "swift://" and the container name from the URI"
    # Also remove the leading delimiter.
    object_file_with_bucket = URI.split(URI.encode(uri))[5]
    object_file_with_bucket.split(File::Separator)[2..- 1].join(File::Separator)
  end

  def upload_single(dest_uri)
    #
    # Get the remote path, and parse out the bucket name.
    #
    object_file = uri_to_object_path(dest_uri)
    #
    # write dump file to swift
    #
    logger.debug("Writing [#{source_input}] to Bucket [#{@container_name}] using object file name [#{object_file}]")
    begin
      params = { :key => object_file, :body => source_input }
      params[:request_block] = -> { read_single_chunk } if byte_count
      container.files.create(params)
      clear_split_vars
    rescue Excon::Errors::Unauthorized => err
      logger.error("Access to Swift container #{@container_name} failed due to a bad username or password. #{err}")
      msg = "Access to Swift container #{@container_name} failed due to a bad username or password. #{err}"
      raise err, msg, err.backtrace
    rescue => err
      logger.error("Error uploading #{source_input} to Swift container #{@container_name}. #{err}")
      msg = "Error uploading #{source_input} to Swift container #{@container_name}. #{err}"
      raise err, msg, err.backtrace
    end
  end

  def mkdir(_dir)
    container
  end

  def container
    @container ||= begin
                     container = swift.directories.get(container_name)
                     logger.debug("Swift container [#{container}] found")
                     container
                   rescue Fog::Storage::OpenStack::NotFound
                     logger.debug("Swift container #{container_name} does not exist.  Creating.")
                     begin
                       container = swift.directories.create(:key => container_name)
                       logger.debug("Swift container [#{container}] created")
                       container
                     rescue => err
                       logger.error("Error creating Swift container #{container_name}. #{err}")
                       msg = "Error creating Swift container #{container_name}. #{err}"
                       raise err, msg, err.backtrace
                     end
                   rescue => err
                     logger.error("Error getting Swift container #{container_name}. #{err}")
                     msg = "Error getting Swift container #{container_name}. #{err}"
                     raise err, msg, err.backtrace
                   end
  end

  private

  def swift
    require 'manageiq/providers/openstack/legacy/openstack_handle'
    extra_options = {
      :ssl_ca_file    => ::Settings.ssl.ssl_ca_file,
      :ssl_ca_path    => ::Settings.ssl.ssl_ca_path,
      :ssl_cert_store => OpenSSL::X509::Store.new
    }
    extra_options[:domain_id] = @domain_id
    extra_options[:omit_default_port] = ::Settings.ems.ems_openstack.excon.omit_default_port
    extra_options[:read_timeout]      = ::Settings.ems.ems_openstack.excon.read_timeout
    extra_options[:service] = "Compute"

    @osh ||= OpenstackHandle::Handle.new(@username, @password, @host, @port, @api_version, @security_protocol, extra_options)
    @osh.connection_options = {:instrumentor => $fog_log}
    begin
      @swift ||= @osh.swift_service
    rescue Excon::Errors::Unauthorized => err
      logger.error("Access to Swift host #{@host} failed due to a bad username or password. #{err}")
      msg = "Access to Swift host #{@host} failed due to a bad username or password. #{err}"
      raise err, msg, err.backtrace
    rescue => err
      logger.error("Error connecting to Swift host #{@host}. #{err}")
      msg = "Error connecting to Swift host #{@host}. #{err}"
      raise err, msg, err.backtrace
    end
  end

  def query_params(query_string)
    parts = URI.decode_www_form(query_string).to_h
    @region, @api_version, @domain_id, @security_protocol = parts.values_at("region", "api_version", "domain_id", "security_protocol")
  end
end
