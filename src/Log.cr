require "yaml"

module Ph
  class Log
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter path : String

    @[YAML::Field(ignore: true)]
    getter fs : Array(File) = [] of File

    def after_initialize
      Dir.mkdir_p @path

      @fs = Dir.glob("#{@path}/*.log").sort.map { |p| File.open p, "a" }
      @fs = [File.open Ph.filepath(@path, 0, "log"), "a"] if @fs.empty?
      @fs.each { |f| f.sync = true }
    end

    def read(&)
      @fs.each do |_f|
        File.open _f.path do |f|
          loop do
            begin
              k = (Ph.read f).not_nil!
              v = Ph.read f
              yield({k, v})
            rescue IO::EOFError
              break
            end
          end
        end
      end
    end

    def write(b : Bytes)
      @fs.last.write b
    end

    def rotate
      o = @fs.last
      n = File.open Ph.filepath(@path, @fs.size, "log"), "a"
      n.sync = true
      @fs[-1] = n
      o.delete
    end
  end
end
