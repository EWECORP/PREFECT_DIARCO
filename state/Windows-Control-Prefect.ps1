prefect version
prefect work-pool ls
prefect worker ls
Get-Service *prefect*
Get-ScheduledTask | Where-Object {$_.TaskName -like "*prefect*"}
Get-EventLog -LogName Application -Newest 200 | Where {$_.Message -like "*prefect*"}
