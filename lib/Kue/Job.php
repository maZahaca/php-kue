<?php
/**
 * Job.php.
 */

namespace Kue;

use Pagon\Fiber;

class Job extends Fiber
{
    protected static $PRIORITIES = [
        'low' => 10,
        'normal' => 0,
        'medium' => -5,
        'high' => -10,
        'critical' => -15,
    ];

    /**
     * @var Kue
     */
    public $queue;

    /**
     * @var \Redis
     */
    public $client;

    /**
     * Create job
     *
     * @param array $type
     * @param array $data
     *
     * @return \Kue\Job
     */
    public function __construct($type, $data = [])
    {
        parent::__construct(
            [
                'id' => null,
                'type' => null,
                'data' => [],
                'result' => null,
                'priority' => 0,
                'progress' => 0,
                'state' => 'inactive',
                'backoff' => null,
                'error' => '',
                'created_at' => '',
                'updated_at' => '',
                'promote_at' => 0,
                'failed_at' => '',
                'duration' => 0,
                'timing' => 0,
                'attempts' => 0,
                'max_attempts' => 1,
            ]
        );

        $this->injectors['id'] = sha1(uniqid());
        $this->injectors['type'] = $type;
        $this->injectors['data'] = $data;
        $this->injectors['created_at'] = Util::now();

        $this->queue = Kue::$instance;
        $this->client = Kue::$instance->client;
    }

    /**
     * Load job
     *
     * @param string $id
     *
     * @return bool|Job
     */
    public static function load($id)
    {
        $client = Kue::$instance->client;

        if (!$data = $client->hGetAll('q:job:'.$id)) {
            return false;
        }

        if (array_key_exists('data', $data)) {
            $data['data'] = json_decode($data['data'], true);
        }
        if (array_key_exists('result', $data)) {
            $data['result'] = json_decode($data['result'], true);
        }
        if (array_key_exists('backoff', $data)) {
            $data['backoff'] = json_decode($data['backoff'], true);
        }
        $job = new self($data['type'], null);
        $job->append($data);

        return $job;
    }

    /**
     * Set priority
     *
     * @param string|int $pri
     *
     * @return self
     */
    public function priority($pri)
    {
        if (is_numeric($pri)) {
            $this->injectors['priority'] = $pri;
        } else {
            if (isset(self::$PRIORITIES[$pri])) {
                $this->injectors['priority'] = self::$PRIORITIES[$pri];
            }
        }

        return $this;
    }

    /**
     * Attempt by function
     *
     * @param $fn
     *
     * @return self
     */
    public function attempt($fn)
    {
        $max = $this->get('max_attempts');
        $attempts = $this->client->hIncrBy('q:job:'.$this->injectors['id'], 'attempts', 1);
        $fn(max(0, $max - $attempts + 1), $attempts - 1, $max);

        return $this;
    }

    /**
     * Set max attempts
     *
     * @param int $num
     *
     * @return self
     */
    public function attempts($num)
    {
        $plus = $num - $this->injectors['max_attempts'];
        $this->injectors['max_attempts'] += $plus;

        return $this;
    }

    /**
     * @param int $remainingAttempts
     * @param int $attemptNumber
     *
     * @return self
     */
    public function reattempt($remainingAttempts, $attemptNumber)
    {
        if ($remainingAttempts) {
            $backoffFn = $this->getBackoffImpl();
            if ($backoffFn instanceof \Closure) {
                $delay = $backoffFn($attemptNumber);
                $this->delay($delay);

                return $this;
            }

            $this->inactive();
        } else {
            $this->failed();
        }

        return $this;
    }

    /**
     * Timing job
     *
     * @param int|string $time
     *
     * @return self
     */
    public function timing($time)
    {
        if (!is_numeric($time)) {
            $time = strtotime($time);
        }

        if (!$this->queue->originalMode()) {
            $this->injectors['timing'] = $time * 1000;
            $this->injectors['state'] = 'inactive';
        } else {
            $this->delay($time - time());
        }

        return $this;
    }

    /**
     * @param array $param
     *
     * @return $this
     */
    public function backoff($param = null)
    {
        if ($param === null) {
            return $this->injectors['backoff'];
        }
        $this->injectors['backoff'] = $param;

        return $this;
    }

    private function getBackoffImpl()
    {
        $supportedBackoffs = [
            'fixed' => function ($delay) {
                return function ($attempts) use ($delay) {
                    return $delay;
                };
            },
            'exponential' => function ($delay) {
                return function ($attempts) use ($delay) {
                    return round($delay * 0.5 * (pow(2, $attempts) - 1));
                };
            },
        ];

        if (is_array($this->injectors['backoff'])) {
            $delay = isset($this->injectors['backoff']['delay']) ?
                $this->injectors['backoff']['delay'] :
                $this->injectors['delay'];

            return $supportedBackoffs[$this->injectors['backoff']['type']]($delay);
        }

        return $this->injectors['backoff']['type'];
    }

    /**
     * Set job delay
     *
     * @param int $s Delay time in seconds
     *
     * @return self
     */
    public function delay($s)
    {
        if (!$this->queue->originalMode()) {
            $this->timing(time() + $s);
        } else {
            $this->injectors['delay'] = $s * 1000;
            $this->injectors['state'] = 'delayed';
        }

        return $this;
    }

    /**
     * Set progress
     *
     * @param int|float $pt
     *
     * @return self
     */
    public function progress($pt)
    {
        $this->set('progress', min(100, ($pt < 1 ? $pt * 100 : $pt)));
        $this->set('updated_at', Util::now());

        return $this;
    }

    /**
     * Set error
     *
     * @param string $error
     *
     * @return self
     */
    public function error($error = null)
    {
        if ($error === null) {
            return $this->injectors['error'];
        }

        $this->emit('error', $error);

        if ($error instanceof \Exception) {
            $str = $error->getMessage() . " \n " . get_class($error).' Error on '.$error->getFile().' '.$error->getLine();
            $str .= $error->getTraceAsString();
        } else {
            $str = $error;
        }

        $this->set('error', $str);
        $this->set('failed_at', Util::now());

        return $this;
    }

    /**
     * Set complete
     *
     * @param mixed $result
     *
     * @return self
     */
    public function complete($result = null)
    {
        if ($result) {
            $this->set('result', json_encode($result));
        }

        return $this->set('progress', 100)->state('complete');
    }

    /**
     * Set failed
     *
     * @return self
     */
    public function failed()
    {
        return $this->state('failed');
    }

    /**
     * Set inactive
     *
     * @return self
     */
    public function inactive()
    {
        return $this->state('inactive');
    }

    /**
     * Set active
     *
     * @return self
     */
    public function active()
    {
        return $this->state('active');
    }

    /**
     * Remove all state from sorted sets
     *
     * @return self
     */
    public function removeState()
    {
        $this->client->zRem('q:jobs', $this->injectors['id']);
        $this->client->zRem('q:jobs:'.$this->injectors['state'], $this->injectors['id']);
        $this->client->zRem('q:jobs:'.$this->injectors['type'].':'.$this->injectors['state'], $this->injectors['id']);

        return $this;
    }

    /**
     * Remove job from storage
     *
     * @return self
     */
    public function remove()
    {
        $this->removeState();

        $this->client->del('q:jobs:'.$this->injectors['id'].':log', 'q:jobs:'.$this->injectors['id']);

        $this->emit('remove', $this->id, $this->type);

        return $this;
    }

    /**
     * Change state
     *
     * @param $state
     *
     * @return self
     */
    public function state($state)
    {
        $this->emit($state);
        $this->removeState();

        // Keep "FIFO!"
        $score = $this->injectors['timing'] + $this->injectors['priority'];

        $this->set('state', $state);
        $this->client->zAdd('q:jobs', $score, $this->injectors['id']);
        $this->client->zAdd('q:jobs:'.$state, $score, $this->injectors['id']);
        $this->client->zAdd('q:jobs:'.$this->injectors['type'].':'.$state, $score, $this->injectors['id']);

        // Set inactive job to waiting list
        if ($this->queue->originalMode() && 'inactive' == $state) {
            $this->client->lPush('q:'.$this->injectors['type'].':jobs', 1);
        }

        $this->set('updated_at', Util::now());

        return $this;
    }

    /**
     * Update the job
     *
     * @return self
     */
    public function update()
    {
        if (!$this->injectors['id']) {
            return $this;
        }

        $this->emit('update');
        $this->injectors['updated_at'] = Util::now();

        $job = $this->injectors;
        $job['data'] = json_encode($job['data']);
        $this->client->hMset('q:job:'.$this->injectors['id'], $job);
        $this->state($job['state']);

        return $this;
    }

    /**
     * Write job log
     *
     * @param string $str
     *
     * @return self
     */
    public function log($str)
    {
        $this->emit('log', $str);
        $this->client->rPush('q:job:'.$this->injectors['id'].':log', $str);
        $this->set('updated_at', Util::now());

        return $this;
    }

    /**
     * Get job property
     *
     * @param string $key
     *
     * @return mixed
     */
    public function get($key)
    {
        return $this->client->hGet('q:job:'.$this->injectors['id'], $key);
    }

    /**
     * Set job property
     *
     * @param string $key
     * @param string $val
     *
     * @return self
     */
    public function set($key, $val)
    {
        $this->injectors[$key] = $val;
        $this->client->hSet('q:job:'.$this->injectors['id'], $key, $val);

        return $this;
    }

    /**
     * Save the job
     *
     * @return self
     */
    public function save()
    {
        $this->emit('save');
        $this->update();

        $this->client->sAdd('q:job:types', $this->injectors['type']);

        return $this;
    }
}
