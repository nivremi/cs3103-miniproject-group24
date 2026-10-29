# Ubuntu Setup for LiveRecall

Ubuntu Linux is required for the reproducible performance/network-impairment tests because they use Linux `tc/netem`.

## Existing Ubuntu / Mininet VM

You may reuse your earlier Ubuntu environment if these commands work:

```bash
python3 --version
tc -V
```

Install the mandatory packages if needed:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
```

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Optional Multipass on macOS or Windows

Official installation page: https://canonical.com/multipass/install

Example after Multipass is installed:

```bash
multipass launch --name liverecall --cpus 2 --memory 4G --disk 15G
multipass shell liverecall
```

Inside Ubuntu:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
```

When finished, exit the VM shell and run on the host:

```bash
multipass stop liverecall
```

Run the server and simulated clients inside the same Ubuntu environment for the public performance experiments. The supplied profiles can then be applied to `lo`.

`ffmpeg` and ASR packages are **not** mandatory dependencies. Install them only if your team attempts the optional ASR bonus.
