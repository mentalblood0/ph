require "yaml"

module Ph
  class Log
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter path : Path

    @[YAML::Field(ignore: true)]
    @f : File = File.new File::NULL, "a"

    def after_initialize
      Dir.mkdir_p @path.parent
      @f = File.open path, "a"
      @f.sync = true
    end

    def read(&)
      File.open @f.path do |f|
        loop do
          begin
            k = (Ph.read f).as Bytes
            v = (Ph.read f).as Bytes?
            yield({k, v})
          rescue IO::EOFError
            break
          end
        end
      end
    end

    def write(b : Bytes)
      @f.write b
    end

    def truncate
      @f.truncate
    end
  end
end
