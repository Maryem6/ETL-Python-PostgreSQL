# Pipeline scheduling

To schedule your script to run every first day of the month at 7 AM using cron, you need to create a cron job with the appropriate time specification. Here are the steps to set up your cron job:

1. Open the crontab editor:

```bash
crontab -e
```

2. Add the cron job by adding the following line to the crontab file to schedule your script:

```bash
0 7 1 * * /home/maryem/etl-script.sh
```

Here is a breakdown of the cron syntax:
- 0 – The minute field (0th minute).
- 7 – The hour field (7 AM).
- 1 – The day of the month field (1st day).
- * – The month field (every month).
- * – The day of the week field (every day of the week).
- /path/to/your/script.sh – The full path to your script.

---------------------
> [!Note]
> **Job scheduling syntax:**
> ```
> m  h  dom  mon  dow  command
> ```
> (minute, hour, day of month, month, day of week)
---------------------
> [!Tip]
You can use the `*` wildcard to mean "any"a
---------------------

3. To deploy your cron job, save the changes and exit

**Note:** Make sure your script has execute permissions. You can set the permissions using the chmod command:

4. To list all cron jobs:

```bash
crontab -l
```
