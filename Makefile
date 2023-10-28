
boot:
	start-dfs.sh
	start-yarn.sh
	start-master.sh
	start-workers.sh

unboot:
	stop-dfs.sh
	stop-yarn.sh
	stop-master.sh
	stop-workers.sh
