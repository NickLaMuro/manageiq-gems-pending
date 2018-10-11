require "active_record"
require 'more_core_extensions/core_ext/hash'

# Some config settings in the helper class are class methods instead of
# constants to allow them to be lazy loaded, instead of loaded right away,
# allowing the PostgresRunner to boot up `postgres` and set it's values if
# necessary.
class RestoreHelper
  SPEC_DIR      = File.expand_path(File.join("..", ".."), __dir__)
  LIB_DIR       = File.expand_path(File.join("..", "lib"), SPEC_DIR)
  PG_DUMPFILE   = File.join(SPEC_DIR, "util", "data", "pg_dump.gz")
  PG_BACKUPFILE = File.join(SPEC_DIR, "util", "data", "pg_backup.tar.gz")

  def self.pg_port
    @pg_port ||= defined?(PostgresRunner) ? PostgresRunner.port : nil
  end

  def self.default_restore_dump_opts
    @default_restore_dump_opts ||= {
      :local_file => PG_DUMPFILE,
      :username   => "root",
      :password   => "smartvm",
      :hostname   => "localhost",
      :port       => pg_port
    }.delete_blanks
  end

  def self.restore_backup_opts
    { :local_file => PG_BACKUPFILE }
  end
end

# Required that you define `db_name` yourself in your specs
shared_context "Database Restore Validation Helpers" do
  let(:active_record_connection_info) do
    {
      "adapter"  => "postgresql",
      "encoding" => "utf8",
      "host"     => "localhost",
      "port"     => RestoreHelper.pg_port,
      "username" => "root",
      "password" => "smartvm",
      "database" => dbname
    }.delete_blanks
  end

  def author_count
    connect
    get_count("SELECT COUNT(id) FROM authors")
  end

  def book_count
    connect
    get_count("SELECT COUNT(id) FROM books")
  end

  # Just connect every time instead of trying to figure out if we have the
  # proper connection.  With the database being rebooted on pg_basebackup
  # restores and the connection becoming potentially invalid in certain
  # scenarios, it is simpler to just reconnect for each test instead of trying
  # to handle all of the edge cases.
  #
  # There are only a few queries in total, so it isn't a big performance hit to
  # do this.  Doing this also avoids dealing with some extra "FATAL" logs
  # showing up in the test output from a invalid connection trying to used.
  def connect
    ActiveRecord::Base.establish_connection active_record_connection_info
  end

  def get_count(sql)
    ActiveRecord::Base.connection.select_value(sql)
  end

  def set_spec_env_for_postgres_admin_basebackup_restore
    if defined?(PostgresRunner)
      PostgresRunner.set_env # can be reset by other specs
      allow(PostgresAdmin).to receive(:user).and_return(PostgresRunner.user)
      allow(PostgresAdmin).to receive(:group).and_return(PostgresRunner.group)
      allow(LinuxAdmin::Service).to receive(:new).with(PostgresAdmin.service_name)
                                                 .and_return(PostgresRunner)
    elsif ENV["CI"]
      # Travis uses upstart for our current 'trusty' instance, so this will
      # just be stubbed like the above, so the sevice name is just something
      # that we can recognize.  See:
      #
      #   https://github.com/travis-ci/travis-build/blob/4f580b2/lib/travis/build/bash/travis_setup_postgresql.bash#L26-L27
      #   https://github.com/travis-ci/travis-cookbooks/blob/46a8e7fd/cookbooks/travis_postgresql/templates/ubuntu/9.5/postgresql.conf.erb#L41-L45
      #
      # Unfortunately the plot thickens a bit since they do two things that is
      # different from our appliance installs:
      #
      #   - 'RAMFS' is used for their data directory, but is sync'd from
      #     /var/lib/postgresql/9.5/main via their init scripts
      #   - The configs and data are in separate dirs (/etc and /var/lib
      #     respectively)
      #
      # An example of the running `postgres` command can be found below
      #
      #   $ /usr/lib/postgresql/9.5/bin/postgres                       \
      #       -D /var/ramfs/postgresql/9.5/main                        \
      #       -c config_file=/etc/postgresql/9.5/main/postgresql.conf
      #
      # The other issue we run into is that we basically require `sudo` for
      # most of our actions in `PostgresAdmin`, and specs are run using the
      # un-elevated CI user, travis.
      #
      # The command below basically runs the the PostgresAdmin command in a
      # subprocess with elevated privleges, instead of trying to stub
      # everything and correct it in a case by case basis.
      env = {
        "APPLIANCE_PG_DATA"    => "/var/lib/postgresql/9.5/main",
        "APPLIANCE_PG_SERVICE" => "ci_pg_instance"
      }

      dir_opts     = "-I #{RestoreHelper::SPEC_DIR} -I #{RestoreHelper::LIB_DIR}"
      require_opts = "-r linux_admin -r ci_helper -r gems/pending/util/postgres_admin"
      ruby_eval    = "-e 'PostgresAdmin.restore(#{RestoreHelper.restore_backup_opts.inspect})'"
      cmd          = "sudo #{Gem.ruby} #{dir_opts} #{require_opts} #{ruby_eval}"

      allow(PostgresAdmin).to receive(:restore).and_wrap_original do |_, *_args|
        system(env, cmd)
      end
    end
  end
end
