# Cloud-Computing

  **KVStore:** Designed and implemented a simple key-value store which is similar to Memcached lite <br />
  **Map-Reduce:** Designed and implemented a distributed MapReduce system. <br />
  **The Map-Reduce system and KVStore have been deployed on Google Cloud.** <br /> <br/>
  **Description:** The KVStore runs on a VM in the background. The Map-Reduce system launches the mapper nodes dynamically and terminated upon completion of mapper tasks. The start-up scripts on mapper VMs contains all the tasks that needs to be carried out. Upon completion of mapper tasks, the master node launches the reducer nodes dynamically and terminates the same upon completion. Map-Reduce system talks to the KVStore to store the intermittent key-value pairs.
