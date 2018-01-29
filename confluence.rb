require 'json'

class Confluence
  def initialize()
    load_config
  end

  def load_config()
    @conf = open('./confluence_config.json') { |io|
      JSON.parse(io.read, symbolize_names: true)
    }
  end

  def execute(options)
    command = 'sh ./confluence.sh -s %{site} -u %{user} -p %{password}' % @conf
    options.each do |k, v|
      command += " #{k.length == 1 ? "-#{k}" : "--#{k}"} #{v}"
    end
    #return `command`
    return system(command)
  end
end

