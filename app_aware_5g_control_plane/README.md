### DPDK Setup

Data Plane Development Kit (DPDK) is a library to accelerate the packet processing workloads. In order to install DPDK, you need to perform the following steps on all three servers:

1. Download DPDK 17.11 by running the following command: <br/> `wget https://github.com/DPDK/dpdk/archive/v17.11.tar.gz`
2. Extract the `.tar.gz` file in a folder.
3. Navigate to `dpdk-17.11/usertools`.
4. Execute `dpdk-setup.sh` file using the command: <br/>
`sudo ./dpdk-setup.sh`
5. From the `Step 1: Select the DPDK environment to build` section, select right system architecture and compiler. For us it was _[14] x86_64-native-linuxapp-gcc_
6. Insert the driver, we used _[17] Insert IGB UIO module_
7. Setup hugepages, we used NUMA system so _[21] Setup hugepage mappings for NUMA systems_.
The number of huge pages depends on the system's memory. We used to run experiments with 30,000 hugepages, each of 2kB. Later on, we used 1GB hugepages, total of 100.
8. Bind ports, _[23] Bind Ethernet/Crypto device to IGB UIO module_.
9. Lastly, add following lines to the `/etc/environment` file:
```
RTE_SDK="path_to_dpdk/dpdk-17.11"
RTE_TARGET="x86_64-native-linuxapp-gcc"
```

### External Libraries

The CTA and CPF use a C++ [Libconfig](https://github.com/hyperrealm/libconfig) library to manage the system's configuration files. In order to install it, you need to execute following commands:

```
sudo apt-get update -y
sudo apt-get install -y libconfig-dev
```

### CellClone Setup

Perform the following steps to setup CellClone:
1. Clone the CellClone repository on all three servers.
2. At the Control Traffic Generator server, navigate to [Control Traffic Generator](https://github.com/nsgLUMS/neutrinoPrivate/tree/prioritization/src/pktgen) folder and run the following commands:
```
sudo make clean
sudo make
```
3. At the CTA server, navigate to [CTA](https://github.com/nsgLUMS/neutrinoPrivate/tree/prioritization/src/core/cta) folder and run the `sudo make clean && sudo make`.
4. Similarly, at the CPF server, navigate to [CPF](https://github.com/nsgLUMS/neutrinoPrivate/tree/prioritization/src/core/cpf) folder and execute `sudo make && sudo make`.

### Servers Configuration

Open [servers_configuration.json.sample](https://github.com/nsgLUMS/neutrinoPrivate/blob/prioritization/src/pktgen/servers_credentials.json.sample) and fill out the credentials of your servers. After that, rename `servers_credentials.json.sample` to `servers_credentials.json`.

### Experiments Execution

To execute an experiment, navigate to [pktgen](https://github.com/nsgLUMS/neutrinoPrivate/tree/prioritization/src/pktgen) and execute `sudo python3 run_experiments.py --clean --cta --pktgen` command. If you want to change experiment configuration, open `config.json` file in the same folder.
