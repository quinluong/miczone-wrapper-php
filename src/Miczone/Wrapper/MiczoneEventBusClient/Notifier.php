<?php

namespace Miczone\Wrapper\MiczoneEventBusClient;

use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Miczone\Thrift\EventBus\MessageInfoBoolean;
use Miczone\Thrift\EventBus\MessageInfoDouble;
use Miczone\Thrift\EventBus\MessageInfoInteger;
use Miczone\Thrift\EventBus\MessageInfoString;
use Miczone\Thrift\EventBus\MiczoneEventBusServiceClient;
use Miczone\Thrift\EventBus\NotifyMessageBooleanRequest;
use Miczone\Thrift\EventBus\NotifyMessageDoubleRequest;
use Miczone\Thrift\EventBus\NotifyMessageIntegerRequest;
use Miczone\Thrift\EventBus\NotifyMessageResponse;
use Miczone\Thrift\EventBus\NotifyMessageStringRequest;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Notifier {
  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const CLIENT_ID = '';

  const AUTH = '';

  const TOPICS = [];

  const SEND_TIMEOUT_IN_MILLISECONDS = 1000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 1000;

  const NUMBER_OF_RETRIES = 1;

  protected $config;

  protected $operationHandle;

  protected $lastException;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'clientId' => static::CLIENT_ID,
      'auth' => static::AUTH,
      'topics' => static::TOPICS,
      'sendTimeoutInMilliseconds' => static::SEND_TIMEOUT_IN_MILLISECONDS,
      'receiveTimeoutInMilliseconds' => static::RECEIVE_TIMEOUT_IN_MILLISECONDS,
      'numberOfRetries' => static::NUMBER_OF_RETRIES,
    ], $config);

    $config['hosts'] = $this->_standardizeHosts($config['hosts']);

    if (empty($config['hosts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    if (empty($config['clientId'])) {
      throw new \Exception('Invalid "clientId" config');
    }

    $config['auth'] = $this->_standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
    }

    $config['topics'] = $this->_standardizeTopics($config['topics']);

    if (empty($config['topics'])) {
      throw new \Exception('Invalid "topics" config');
    }

    if (!is_int($config['sendTimeoutInMilliseconds']) || $config['sendTimeoutInMilliseconds'] <= 0) {
      $config['sendTimeoutInMilliseconds'] = static::SEND_TIMEOUT_IN_MILLISECONDS;
    }

    if (!is_int($config['receiveTimeoutInMilliseconds']) || $config['receiveTimeoutInMilliseconds'] <= 0) {
      $config['receiveTimeoutInMilliseconds'] = static::RECEIVE_TIMEOUT_IN_MILLISECONDS;
    }

    if (!is_int($config['numberOfRetries']) || $config['numberOfRetries'] <= 0) {
      $config['numberOfRetries'] = static::NUMBER_OF_RETRIES;
    }

    $this->config = $config;

    $this->operationHandle = new OperationHandle([
      'username' => $this->config['auth']['username'],
      'password' => $this->config['auth']['password'],
    ]);
  }

  public function __destruct() {
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
      array_push($result, [
        'host' => $host,
        'port' => $port,
      ]);
    }

    return $result;
  }

  private function _standardizeAuth(string $auth) {
    if (empty($auth)) {
      return [];
    }

    $auth = explode(':', $auth);

    if (count($auth) !== 2) {
      return [];
    }

    $username = trim($auth[0]);
    $password = trim($auth[1]);

    if (empty($username) || empty($password)) {
      return [];
    }

    $result = [
      'username' => $username,
      'password' => $password,
    ];

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

  private function _createNotifyMessageBooleanRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->config['topics'])) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoBoolean();
    $messageInfo->clientId = $this->config['clientId'];
    $messageInfo->topic = $params['topic'];
    if (isset($params['data'])) {
      if (!is_bool($params['data'])) {
        throw new \Exception('Invalid "data" param');
      } else {
        $messageInfo->data = $params['data'];
      }
    }
    if (isset($params['key'])) {
      if (!is_string($params['key'])) {
        throw new \Exception('Invalid "key" param');
      } else {
        $messageInfo->key = $params['key'];
      }
    }

    $request = new NotifyMessageBooleanRequest([
      'messageInfo' => $messageInfo,
    ]);

    return $request;
  }

  private function _createNotifyMessageIntegerRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->config['topics'])) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoInteger();
    $messageInfo->clientId = $this->config['clientId'];
    $messageInfo->topic = $params['topic'];
    if (isset($params['data'])) {
      if (!is_int($params['data'])) {
        throw new \Exception('Invalid "data" param');
      } else {
        $messageInfo->data = $params['data'];
      }
    }
    if (isset($params['key'])) {
      if (!is_string($params['key'])) {
        throw new \Exception('Invalid "key" param');
      } else {
        $messageInfo->key = $params['key'];
      }
    }

    $request = new NotifyMessageIntegerRequest([
      'messageInfo' => $messageInfo,
    ]);

    return $request;
  }

  private function _createNotifyMessageDoubleRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->config['topics'])) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoDouble();
    $messageInfo->clientId = $this->config['clientId'];
    $messageInfo->topic = $params['topic'];
    if (isset($params['data'])) {
      if (!is_double($params['data'])) {
        throw new \Exception('Invalid "data" param');
      } else {
        $messageInfo->data = $params['data'];
      }
    }
    if (isset($params['key'])) {
      if (!is_string($params['key'])) {
        throw new \Exception('Invalid "key" param');
      } else {
        $messageInfo->key = $params['key'];
      }
    }

    $request = new NotifyMessageDoubleRequest([
      'messageInfo' => $messageInfo,
    ]);

    return $request;
  }

  private function _createNotifyMessageStringRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->config['topics'])) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoString();
    $messageInfo->clientId = $this->config['clientId'];
    $messageInfo->topic = $params['topic'];
    if (isset($params['data'])) {
      if (!is_string($params['data']) || empty($params['data'])) {
        throw new \Exception('Invalid "data" param');
      } else {
        $messageInfo->data = $params['data'];
      }
    }
    if (isset($params['key'])) {
      if (!is_string($params['key'])) {
        throw new \Exception('Invalid "key" param');
      } else {
        $messageInfo->key = $params['key'];
      }
    }

    $request = new NotifyMessageStringRequest([
      'messageInfo' => $messageInfo,
    ]);

    return $request;
  }

  private function _createTransportAndClient($host, $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->config['sendTimeoutInMilliseconds']);
    $socket->setRecvTimeout($this->config['receiveTimeoutInMilliseconds']);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneEventBusServiceClient($protocol);

    return [$transport, $client];
  }

  public function getLastException() {
    return $this->lastException;
  }

  /**
   * @return \Miczone\Thrift\Common\ErrorCode
   */
  public function ping() {
    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->ping();
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return ErrorCode::THRIFT_BAD_REQUEST;
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * bool     data
   * string   key
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyBoolean(array $params = []) {
    $request = $this->_createNotifyMessageBooleanRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->notifyBoolean($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new NotifyMessageResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * int      data
   * string   key
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyInteger(array $params = []) {
    $request = $this->_createNotifyMessageIntegerRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->notifyInteger($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new NotifyMessageResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * double   data
   * string   key
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyDouble(array $params = []) {
    $request = $this->_createNotifyMessageDoubleRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->notifyDouble($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new NotifyMessageResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * string   data
   * string   key
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyString(array $params = []) {
    $request = $this->_createNotifyMessageStringRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->notifyString($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new NotifyMessageResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * bool     data
   * string   key
   * @throws \Exception
   */
  public function ow_notifyBoolean(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageBooleanRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_notifyBoolean($this->operationHandle, $request);
          $transport->close();

          if (is_callable($successCallback)) {
            call_user_func($successCallback);
          }

          return;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    if (is_callable($errorCallback)) {
      call_user_func($errorCallback, $this->lastException);
    }
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * int      data
   * string   key
   * @throws \Exception
   */
  public function ow_notifyInteger(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageIntegerRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_notifyInteger($this->operationHandle, $request);
          $transport->close();

          if (is_callable($successCallback)) {
            call_user_func($successCallback);
          }

          return;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    if (is_callable($errorCallback)) {
      call_user_func($errorCallback, $this->lastException);
    }
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * double   data
   * string   key
   * @throws \Exception
   */
  public function ow_notifyDouble(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageDoubleRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_notifyDouble($this->operationHandle, $request);
          $transport->close();

          if (is_callable($successCallback)) {
            call_user_func($successCallback);
          }

          return;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    if (is_callable($errorCallback)) {
      call_user_func($errorCallback, $this->lastException);
    }
  }

  /**
   * @param array $params Contains:
   * string   topic (required)
   * string   data
   * string   key
   * @throws \Exception
   */
  public function ow_notifyString(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageStringRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_notifyString($this->operationHandle, $request);
          $transport->close();

          if (is_callable($successCallback)) {
            call_user_func($successCallback);
          }

          return;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    if (is_callable($errorCallback)) {
      call_user_func($errorCallback, $this->lastException);
    }
  }

}
