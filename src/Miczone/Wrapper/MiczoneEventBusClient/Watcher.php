<?php

namespace Miczone\Wrapper\MiczoneEventBusClient;

use Miczone\Thrift\Common\ErrorCode;
use Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler\BooleanWatcherHandlerInterface;
use Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler\DoubleWatcherHandlerInterface;
use Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler\IntegerWatcherHandlerInterface;
use Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler\StringWatcherHandlerInterface;

class Watcher {
  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const CLIENT_ID = '';

  const GROUP_ID = '';

  const TOPICS = [];

  protected $config;

  protected $consumer;

  protected $isRunning = false;

  protected $handler;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'clientId' => static::CLIENT_ID,
      'groupId' => static::GROUP_ID,
      'topics' => static::TOPICS,
    ], $config);

    $config['hosts'] = $this->_standardizeHosts($config['hosts']);

    if (empty($config['hosts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    if (empty($config['clientId'])) {
      throw new \Exception('Invalid "clientId" config');
    }

    if (empty($config['groupId'])) {
      throw new \Exception('Invalid "groupId" config');
    }

    $config['topics'] = $this->_standardizeTopics($config['topics']);

    if (empty($config['topics'])) {
      throw new \Exception('Invalid "topics" config');
    }

    $this->config = $config;
  }

  public function __destruct() {
  }

  private function _createRdKafkaConf() {
    $servers = implode(',', $this->config['hosts']);

    $topicConf = new \RdKafka\TopicConf();
    $topicConf->set('auto.offset.reset', 'latest'); // smallest, earliest, beginning, largest, latest, end, error

    $conf = new \RdKafka\Conf();
    $conf->set('bootstrap.servers', $servers);
    $conf->set('client.id', $this->config['clientId']);
    $conf->set('group.id', $this->config['groupId']);
    $conf->set('enable.auto.commit', true);
    $conf->set('auto.commit.interval.ms', 1000);
    $conf->set('queued.max.messages.kbytes', 65536);
    $conf->setDefaultTopicConf($topicConf);

    return $conf;
  }

  private function _standardizeHosts(string $hosts) {
    if (empty($hosts)) {
      return [];
    }

    $hosts = explode(',', $hosts);

    if (empty($hosts)) {
      return [];
    }

    $result = [];

    foreach ($hosts as $item) {
      $hostPortPair = explode(':', $item);
      if (count($hostPortPair) !== 2) {
        continue;
      }
      $host = trim($hostPortPair[0]);
      $port = (int) trim($hostPortPair[1]);
      if (empty($host) || $port <= 0) {
        continue;
      }
      array_push($result, $host . ':' . $port);
    }

    return $result;
  }

  private function _standardizeTopics(string $topics) {
    if (empty($topics)) {
      return [];
    }

    $topics = explode(',', $topics);

    if (empty($topics)) {
      return [];
    }

    $result = [];

    foreach ($topics as $topic) {
      $topic = trim($topic);
      if (empty($topic)) {
        continue;
      }
      array_push($result, $topic);
    }

    return $result;
  }

  private function _error($code, string $message, \Exception $exception = null) {
    if (!\method_exists($this->handler, 'onError')) {
      return;
    }

    if ($exception === null) {
      $this->handler->onError($code, $message);
    } else {
      $this->handler->onError($code, $message, $exception);
    }
  }

  private function _process($message) {
    $topic = $message->topic_name;
    $partition = $message->partition;
    $offset = $message->offset;
    $timestamp = $message->timestamp;
    $key = $message->key;
    $data = null;

    if (isset($message->payload) && $message->payload !== null) {
      $data = $message->payload;
    }

    if ($this->handler instanceof BooleanWatcherHandlerInterface) {
      if ($data !== null) {
        $data = boolval($data);
      }
      $this->handler->onMessage($topic, $partition, $offset, $timestamp, $key, $data);
    } else if ($this->handler instanceof IntegerWatcherHandlerInterface) {
      if ($data !== null) {
        $data = intval($data);
      }
      $this->handler->onMessage($topic, $partition, $offset, $timestamp, $key, $data);
    } else if ($this->handler instanceof DoubleWatcherHandlerInterface) {
      if ($data !== null) {
        $data = floatval($data);
      }
      $this->handler->onMessage($topic, $partition, $offset, $timestamp, $key, $data);
    } else if ($this->handler instanceof StringWatcherHandlerInterface) {
      if ($data !== null) {
        $data = strval($data);
      }
      $this->handler->onMessage($topic, $partition, $offset, $timestamp, $key, $data);
    } else {
      $this->_error(ErrorCode::FAIL, 'Can not receive data, data type is not supported');
    }
  }

  public function setHandler($handler) {
    if ($handler === null || !\method_exists($handler, 'onMessage')) {
      throw new \Exception('Invalid handler');
    }

    $this->handler = $handler;
  }

  public function start() {
    if ($this->handler === null) {
      throw new \Exception('Handler is not defined');
    }

    try {
      $conf = $this->_createRdKafkaConf();

      $this->consumer = new \RdKafka\KafkaConsumer($conf);

      $this->consumer->subscribe($this->config['topics']);

      $this->isRunning = true;

      while ($this->isRunning) {
        $message = $this->consumer->consume(120000);

        switch ($message->err) {
          case RD_KAFKA_RESP_ERR_NO_ERROR:
            $this->_process($message);
            break;

          case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            break;

          case RD_KAFKA_RESP_ERR__TIMED_OUT:
            break;

          default:
            break;
        }
      }
    } catch (\Exception $ex) {
      $this->_error($ex->getCode(), $ex->getMessage());
    } finally {
      if ($this->consumer != null) {
        $this->consumer->unsubscribe();
      }

      $this->isRunning = false;
    }
  }

  public function stop() {
    $this->isRunning = false;

    if ($this->consumer != null) {
      $this->consumer->unsubscribe();
    }
  }

}
