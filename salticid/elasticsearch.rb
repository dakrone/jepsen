require 'fileutils'

role :elasticsearch do
  task :setup do
    sudo do
      cd '/opt/'
      unless dir? :elasticsearch
        # install es here
      end
      cd :elasticsearch
      # other things?
    end
  end

  task :start do
    cd '/opt/elasticsearch'
    exec! 'bash -c "bin/elasticsearch -f"', echo: true
  end

  task :sentinel do
    cd '/var/run/'
    exec! 'file elasticsearch.pid'
  end

  task :stop do
    sudo do
      killall 'java' rescue log "no elasticsearch"
    end
  end

  task :replication do
    # no idea what replication does
  end
end
