<?php
require_once(WWW_DIR."/lib/framework/db.php");
require_once(WWW_DIR."/lib/groups.php");
require_once(WWW_DIR."/lib/nntp.php");
require_once(WWW_DIR."/lib/binaries.php");

class Backfill 
{
    function backfillAllGroups($groupName='', $backfillDate=null, $regexOnly=false)
    {
        $groups = new Groups;
        $res = false;
        if ($groupName != '') {
            $grp = $groups->getByName($groupName);
            if ($grp)
                $res = array($grp);
        } else {
            $res = $groups->getActive();
        }

        if ($res) {
            foreach($res as $groupArr) {
                $this->backfillGroup($groupArr, $backfillDate, $regexOnly);
            }
        } else {
            echo "No groups specified. Ensure groups are added to newznab's database for updating.\n";
        }
    }

    function backfillGroup($groupArr, $backfillDate=null, $regexOnly=false)
    {
        $db = new DB();
        $binaries = new Binaries();

        if ($regexOnly === true) {
            echo "Only inserting binaries which match regex\n";
            $binaries->onlyProcessRegexBinaries = true;
        }

        $this->startGroup = microtime(true);

        $nntp = new Nntp();
        $nntpc = new Nntp();
        if ($nntp->doConnect(5, false, true)) {
            echo sprintf("Processing %s\n", $groupArr['name']);

            $data = $nntp->selectGroup($groupArr['name']);
            if ($nntp->isError($data)) {
                echo "Could not select group (bad name?): {$groupArr['name']}\n";
                return;
            }

            if ($backfillDate) {
                $targetpost = $this->daytopost($nntp, $groupArr['name'], $this->dateToDays($backfillDate), true);
            } else {
                $targetpost = $this->daytopost($nntp, $groupArr['name'], $groupArr['backfill_target'], true);
            }

            if ($groupArr['first_record'] == 0 || $groupArr['backfill_target'] == 0 && !$backfillDate) {
                echo "Group {$groupArr['name']} has invalid numbers. Have you run update on it? Have you set the backfill days amount?\n";
                return;
            }

            echo "Group {$data['group']}: server has {$data['first']} - {$data['last']}, or ~";
            echo ((int)((($this->postdate($nntp, $data['last'], false) - (int)$this->postdate($nntp, $data['first'], false)) / 86400)));
            echo " days.\nLocal first = {$groupArr['first_record']} (";
            echo ((int)((date('U') - $this->postdate($nntp, $groupArr['first_record'], false)) / 86400));
            echo " days). Backfill target of " . ($backfillDate ? date('Y-m-d', $backfillDate) : $groupArr['backfill_target'] . " days") . " is post $targetpost.\n";

            if ($targetpost >= $groupArr['first_record']) {
                echo "Nothing to do, we already have the target post.\n \n";
                return "";
            }

            if ($targetpost < $data['first']) {
                echo "WARNING: Backfill came back as before server's first. Setting targetpost to server first.\n";
                echo "Skipping Group \n";
                return "";
            }

            echo ($binaries->onlyProcessRegexBinaries === true) ? "Note: Discarding parts that do not match a regex\n" : "";

            $nntp->doQuit();
            $nntpc->doConnect();
            $datac = $nntpc->selectGroup($groupArr['name']);
            if ($nntpc->isError($datac)) {
                echo "Could not select group (bad name?): {$groupArr['name']}\n";
                return;
            }

            $total = $groupArr['first_record'] - $targetpost;
            $done = false;
            $last = $groupArr['first_record'] - 1;
            $first = $last - $binaries->messagebuffer + 1;
            if ($targetpost > $first) {
                $first = $targetpost;
            }

            while ($done === false) {
                $binaries->startLoop = microtime(true);

                echo "Getting " . ($last - $first + 1) . " parts (" . number_format($first - $targetpost) . " in queue)\n";
                flush();

                $success = $binaries->scan($nntpc, $groupArr, $first, $last, 'backfill');

                if (!$success) {
                    return "";
                }

                $db->exec(sprintf("update `groups` SET first_record = %s, last_updated = now() WHERE ID = %d", $db->escapeString($first), $groupArr['ID']));

                if ($first == $targetpost) {
                    $done = true;
                } else {
                    $last = $first - 1;
                    $first = $last - $binaries->messagebuffer + 1;
                    if ($targetpost > $first) {
                        $first = $targetpost;
                    }
                }
            }

            $nntpc->doQuit();
            $nntp->doConnect();
            $first_record_postdate = $this->postdate($nntp, $first, false);
            $nntp->doQuit();

            if ($first_record_postdate != "") {
                $db->exec(sprintf("update `groups` SET first_record_postdate = FROM_UNIXTIME(%s), last_updated = now() WHERE ID = %d", $first_record_postdate, $groupArr['ID']));
            }

            $timeGroup = number_format(microtime(true) - $this->startGroup, 2);
            echo "Group processed in $timeGroup seconds \n";
        } else {
            echo "Failed to get NNTP connection.\n";
        }
    }

    function postdate($nntp, $post, $debug=true)
    {
        $attempts=0;
        $date="";
        do {
            $msgs = $nntp->getOverview($post."-".$post, true, false);
            if ($nntp->isError($msgs)) {
                echo "Error {$msgs->code}: {$msgs->message}\n";
                echo "Returning from postdate\n";
                return "";
            }

            if (!isset($msgs[0]['Date']) || $msgs[0]['Date'] == "" || is_null($msgs[0]['Date'])) {
                $success = false;
            } else {
                $date = $msgs[0]['Date'];
                $success = true;
            }

            if ($debug && $attempts > 0) echo "retried $attempts time(s)\n";
            $attempts++;
        } while ($attempts <= 3 && $success == false);

        if (!$success) {
            return "";
        }

        $date = strtotime($date);
        return $date;
    }

    function daytopost($nntp, $group, $days, $debug=true)
    {
        $pddebug = false; // DEBUG every postdate call?!?!
        if ($debug) {
            echo "INFO: daytopost finding post for $group $days days back.\n";
        }

        $data = $nntp->selectGroup($group);
        if ($nntp->isError($data)) {
            echo "Error {$data->code}: {$data->message}\n";
            echo "Returning from daytopost\n";
            return "";
        }

        $goaldate = date('U') - (86400 * $days); // goal timestamp
        $firstDate = $this->postdate($nntp, $data['first'], $pddebug);
        $lastDate = $this->postdate($nntp, $data['last'], $pddebug);

        // Check if firstDate or lastDate is numeric
        if (!is_numeric($firstDate) || !is_numeric($lastDate)) {
            echo "Error: Invalid first or last date.\n";
            return "";
        }

        if ($goaldate < $firstDate) {
            echo "WARNING: Backfill target of $days day(s) is older than the first article stored on your news server.\n";
            echo "Starting from the first available article (" . date("r", $firstDate) . " or " . $this->daysOld($firstDate) . " days).\n";
            return $data['first'];
        } elseif ($goaldate > $lastDate) {
            echo "ERROR: Backfill target of $days day(s) is newer than the last article stored on your news server.\n";
            echo "To backfill this group you need to set Backfill Days to at least " . ceil($this->daysOld($lastDate) + 1) . " days (" . date("r", $lastDate - 86400) . ").\n";
            return "";
        }

        $interval = floor(($data['last'] - $data['first']) * 0.5);
        $dateofnextone = $lastDate;

        // Begin while loop
        while ($this->daysOld($dateofnextone) < $days) {
            $nskip = 1;
            while (($tmpDate = $this->postdate($nntp, ($data['last'] - $interval), $pddebug)) > $goaldate) {
                $data['last'] = $data['last'] - $interval - ($nskip - 1);
                if ($debug) {
                    echo "New upperbound ({$data['last']}) is " . $this->daysOld($tmpDate) . " days old.\n";
                }
                $nskip = $nskip * 2;
            }

            $interval = ceil($interval / 2);
            if ($interval <= 0) {
                break;
            }

            $dateofnextone = $this->postdate($nntp, ($data['last'] - 1), $pddebug);
        }

        echo "Determined to be article {$data['last']} which is " . $this->daysOld($dateofnextone) . " days old (" . date("r", $dateofnextone) . ").\n";
        return $data['last'];
    }

    private function daysOld($timestamp)
    {
        if (!is_numeric($timestamp)) 
        {
            echo "Error: Invalid timestamp. Using current time as fallback.";
            $timestamp = time();
        } 
        else 
        {
            $timestamp = (int)$timestamp;
        }

        return round((time() - $timestamp) / 86400, 1);
    }

    private function dateToDays($backfillDate) 
    {
        return floor(-($backfillDate - time()) / (60*60*24));
    }
}
