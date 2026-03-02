# iniminer

iniminer is a cpu mining tool based on a fork of ethminer that uses the stratum protocol over tcp to distribute tasks and submit mining results.

# Nexa2InitVerse

N2I increases the speed of GPU mining INI by 20 times using NexaPoW. Check them in N2I folder.

## Features
- CPU mining
- stratum mining without proxy
- farm failover (getwork + stratum)
- Real-time power tips

## Build & Install

```
cd ini-miner
git submodule update --init

# windows
cmake -G "Visual Studio 15 2017 Win64" .  # will generate a project file for visual studio, then compile it in visual studio

# linux
cmake .
make
```

## Usage

```
./iniminer --pool stratum+tcp://0x0304f5193FCe6A27e3789c27EE2B9D95177e21A5.Worker001@pool-a.yatespool.com:31588

# or specify certain cpus
./iniminer --pool stratum+tcp://0x0304f5193FCe6A27e3789c27EE2B9D95177e21A5.xxx@pool-a.yatespool.com:31588 --cpu-devices 1 --cpu-devices 2
```

## License

Licensed under the [GNU General Public License, Version 3](LICENSE).


