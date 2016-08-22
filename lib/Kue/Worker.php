<?php

namespace Kue;

use Pagon\EventEmitter;

class Worker extends EventEmitter
{
    /**
     * @var Kue
     */
    protected $queue;

    /**
     * @var string
     */
    protected $type;

    /**
     * @var \Redis
     */
    protected $client;

    /**
     * @var int mill seconds
     */
    protected $interval = 1000000;

    /**
     * @var Worker id
     */
    public $id;

    /**
     * Create worker
     *
     * @param Kue    $queue
     * @param string $type
     */
    public function __construct($queue, $type = null)
    {
        $this->queue = $queue;
        $this->type = $type;
        $this->client = $queue->client;
        $this->id = (function_exists('gethostname') ? gethostname() : php_uname('n')).':'.getmypid(
            ).($type ? ':'.$type : '');
    }

    /**
     * Start worker
     */
    public function start()
    {
        while (1) {
            $job = $this->getJob();
            if ($job) {
                $this->process($job);
            }
            usleep($this->interval);
        }
    }

    /**
     * Process the job
     *
     * @param Job $job
     */
    public function process(Job $job)
    {
        $job->active();
        $job->set('worker', $this->id);
        try {
            $start = Util::now();
            $error = $result = null;
            $done = function ($e = null, $res = null) use (&$error, &$result) {
                $error = $e ? $e : null;
                $result = $res ? $res : null;
            };

            $this->queue->emit('process:'.$job->type, $job, $done);

            if ($error) {
                throw $error instanceof \Exception ? $error : new \Exception($error);
            }
            // Retry when failed
            if ($job->state == 'failed') {
                throw new AttemptException("failed to attempts");
            }

            $duration = Util::now() - $start;
            $job->complete($result);
            $job->set('duration', $duration);
        } catch (\Exception $e) {
            if ($e instanceof AttemptException) {
                $e = null;
            }

            $this->failed($job, $e);
        }
    }

    /**
     * Failed trigger
     *
     * @param Job               $job
     * @param string|\Exception $error
     */
    public function failed(Job $job, $error)
    {
        $job->error($error);
        $job->attempt(
            function ($remaining) use ($job) {
                $remaining ? $job->inactive() : $job->failed();
            }
        );
    }

    /**
     * Pop a job
     *
     * @param string $key
     *
     * @return mixed
     */
    public function pop($key)
    {
        $ret = $this->client->zRevRangeByScore($key, Util::now(), '-inf', ['limit' => [0, 1]]);

        if (!$ret) {
            return false;
        }

        // Delete error will lose the control job
        if (!$this->client->zRem($key, $ret[0])) {
            return false;
        }

        return $ret[0];
    }

    /**
     * Get job
     *
     * @return bool|Job
     */
    public function getJob()
    {
        if (!$id = $this->pop('q:jobs:'.($this->type ? $this->type.':' : '').'inactive')) {
            return false;
        }

        if (!$job = Job::load($id)) {
            return false;
        }

        // Compatible for the node.js
        // $this->client->rpop('q:' . $job->type . ':jobs');

        return $job;
    }
}
