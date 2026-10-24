#Vagrantfile actua como plantilla para crear los recursos

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64" #define la imagen para instalar en la mv, compatible mayoria arquitectura bigdata

  #Definicion de nodos
  nodes = [
    { name: "master",  ip: "192.168.56.10", ram: 3072, cpus: 2 },
    { name: "worker1", ip: "192.168.56.11", ram: 2048, cpus: 2 },
    { name: "worker2", ip: "192.168.56.12", ram: 2048, cpus: 2 }
  ]

  # Definición de cada máquina virtual: nombre, hostname, IP privada, memoria RAM y CPU.
  # Se crea una red privada y se asigna una dirección IP a cada nodo dentro de esa red.
  nodes.each do |node|
    config.vm.define node[:name] do |n|
      n.vm.hostname = node[:name]
      n.vm.network "private_network", ip: node[:ip]
      n.vm.provider "virtualbox" do |vb| #indicas motor para levanantar cada maquina virtual
        vb.memory = node[:ram]
        vb.cpus = node[:cpus]
      end
    end
  end

  # El provisioner de vagrant se ejecuta en la última VM para garantizar                                                                                                                                                 
  # que master y worker1 ya están arrancados.                                                                                                                                                                 
  config.vm.define "worker2" do |ctrl|  # cuando se crea worker2                                                                                                                                                                   
    ctrl.vm.provision "ansible" do |ansible|   #configurar ansible en worker2                                                                                                                                                             
      ansible.playbook = "ansible/playbook.yml"                                                                                                                                                               
      ansible.inventory_path = "ansible/inventory.ini"                                                                                                                                                        
      ansible.limit = "all" # aplica la configuracion de ansible de todos los nodos
      ansible.host_key_checking = false  #desactiva verificacion ssh                                                                                                                                                                              
      ansible.compatibility_mode = "2.0" 
    end
  end

end


