<?php

namespace Kue;

use Pagon\Fiber;

class Kue extends Fiber
{
    protected $injectors = [
        'host' => 'localhost',
        'port' => '6379',
        'db' => 0,
        'client' => null,
        'mode' => false,
        'password' => false,
    ];

    /**
     * @var Kue
     */
    public static $instance;

    /**
     * @var \Redis
     */
    public $client;

    /**
     * Create queue for jobs
     *
     * @param array $options
     *
     * @return Kue
     */
    public static function createQueue(array $options = [])
    {
        if (!self::$instance) {
            self::$instance = new self($options);
        }

        return self::$instance;
    }

    /**
     * Set handle error
     */
    public static function handleError()
    {
        set_error_handler(
            function ($type, $message, $file, $line) {
                if (error_reporting() & $type) {
                    throw new \ErrorException($message, $type, 0, $file, $line);
                }
            }
        );
    }

    /**
     * Create queue
     *
     * @param array $options
     *
     * @return Kue
     */
    public function __construct(array $options = [])
    {
        $this->injectors = $options + $this->injectors;

        $this->client = &$this->injectors['client'];

        if (!$this->client) {
            $this->client = new \Redis();
            $this->client->connect($this->injectors['host'], $this->injectors['port']);
            if ($this->injectors['db']) {
                $this->client->select($this->injectors['db']);
            }
            if ($this->injectors['password']) {
                $this->client->auth($this->injectors['password']);
            }
        }
    }

    /**
     * Enable node mode
     *
     * @param boolean $use
     *
     * @return $this
     */
    public function originalMode($use = null)
    {
        if ($use === null) {
            return $this->injectors['mode'] == 'origin';
        }

        $this->injectors['mode'] = $use ? 'origin' : false;

        return $this;
    }

    /**
     * Create jobs
     *
     * @param string $type
     * @param array  $data
     *
     * @return Job
     */
    public function create($type, array $data = [])
    {
        $this->emit('create', $type, $data);

        return new Job($type, $data);
    }

    /**
     * Process with worker
     *
     * @param string   $type
     * @param \Closure $fn
     */
    public function process($type = null, $fn = null)
    {
        if ($type instanceof \Closure) {
            $fn = $type;
            $type = null;
        }
        if ($fn) {
            $this->on('process:'.($type ? $type : '*'), $fn);
        }
        $this->emit('process', $type, $fn);
        $worker = new Worker($this, $type);
        $worker->start();
    }

    /**
     * Get or set setting
     *
     * @param string     $name
     * @param string|int $value
     *
     * @return mixed
     */
    public function setting($name, $value = null)
    {
        if ($value) {
            $this->client->hSet('q:settings', $name, $value);

            return $this;
        }

        return $this->client->hGet('q:settings', $name);
    }

    /**
     * Get all types
     *
     * @return mixed
     */
    public function types()
    {
        return $this->client->sMembers('q:job:types');
    }

    /**
     * Get all by state
     *
     * @param string $state
     *
     * @return mixed
     */
    public function state($state)
    {
        return $this->client->zRange('q:jobs:'.$state, 0, -1);
    }

    /**
     * Get jobs by state
     *
     * @param string $state
     *
     * @return mixed
     */
    public function card($state)
    {
        return $this->client->zCard('q:jobs:'.$state);
    }

    /**
     * Get complete jobs
     *
     * @return mixed
     */
    public function complete()
    {
        return $this->state('complete');
    }

    /**
     * Get failed jobs
     *
     * @return mixed
     */
    public function failed()
    {
        return $this->state('failed');
    }

    /**
     * Get inactive jobs
     *
     * @return mixed
     */
    public function inactive()
    {
        return $this->state('inactive');
    }

    /**
     * Get active jobs
     *
     * @return mixed
     */
    public function active()
    {
        return $this->state('active');
    }
}
