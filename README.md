# iniminer

iniminer is a cpu mining tool based on a fork of ethminer that uses the stratum protocol over tcp to distribute tasks and submit mining results.


## Features
- CPU mining
- stratum mining without proxy
- farm failover (getwork + stratum)
- Real-time power tips

## Build & Install

```
PUKI PUKI PUKI PUKI KONTOLODON

# windows
cmake -G "Visual Studio 15 2017 Win64" .  # will generate a project file for visual studio, then compile it in visual studio

# linux
cmake .
make
```

## Usage

```
./iniminer --pool stratum+tcp://0x0304f5193FCe6A27e3789c27EE2B9D95177e21A5.Worker001@pool-core-testnet.inichain.com:32672

# or specify certain cpus
./iniminer --pool stratum+tcp://0x0304f5193FCe6A27e3789c27EE2B9D95177e21A5.xxx@pool-core-testnet.inichain.com:32672 --cpu-devices 1 --cpu-devices 2
```

## License

Licensed under the [GNU General Public License, Version 3](LICENSE).


