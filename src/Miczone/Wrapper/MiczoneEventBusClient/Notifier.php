<?php

namespace Miczone\Wrapper\MiczoneEventBusClient;

use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Miczone\Thrift\Common\PoolSelectOption;
use Miczone\Thrift\Common\ScaleMode;
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
use Miczone\Wrapper\MiczoneClientBase;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Notifier extends MiczoneClientBase {

  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const CLIENT_ID = '';

  const AUTH = '';

  const TOPICS = [];

  const SEND_TIMEOUT_IN_MILLISECONDS = 1000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 1000;

  const NUMBER_OF_RETRIES = 1;

  const SCALE_MODE = ScaleMode::BALANCING;

  protected $lastException;

  private $_hostPorts;

  private $_hostPortsAliveStatus;

  private $_clientId;

  private $_operationHandle;

  private $_topics;

  private $_sendTimeoutInMilliseconds;

  private $_receiveTimeoutInMilliseconds;

  private $_totalLoop;

  private $_poolSelectOption;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'clientId' => static::CLIENT_ID,
      'auth' => static::AUTH,
      'topics' => static::TOPICS,
      'sendTimeoutInMilliseconds' => static::SEND_TIMEOUT_IN_MILLISECONDS,
      'receiveTimeoutInMilliseconds' => static::RECEIVE_TIMEOUT_IN_MILLISECONDS,
      'numberOfRetries' => static::NUMBER_OF_RETRIES,
      'scaleMode' => static::SCALE_MODE,
    ], $config);

    $config['hostPorts'] = $this->standardizeHosts($config['hosts']);

    if (empty($config['hostPorts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    $this->_hostPorts = $config['hostPorts'];

    $this->_hostPortsAliveStatus = $this->initHostPortsAliveStatus($this->_hostPorts);

    if (empty($config['clientId'])) {
      throw new \Exception('Invalid "clientId" config');
    }

    $this->_clientId = trim($config['clientId']);

    $config['auth'] = $this->standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
    }

    $this->_operationHandle = new OperationHandle([
      'username' => $config['auth']['username'],
      'password' => $config['auth']['password'],
    ]);

    $config['topics'] = $this->standardizeTopics($config['topics']);

    if (empty($config['topics'])) {
      throw new \Exception('Invalid "topics" config');
    }

    $this->_topics = $config['topics'];

    if (!is_int($config['sendTimeoutInMilliseconds']) || $config['sendTimeoutInMilliseconds'] <= 0) {
      $config['sendTimeoutInMilliseconds'] = static::SEND_TIMEOUT_IN_MILLISECONDS;
    }

    $this->_sendTimeoutInMilliseconds = $config['sendTimeoutInMilliseconds'];

    if (!is_int($config['receiveTimeoutInMilliseconds']) || $config['receiveTimeoutInMilliseconds'] <= 0) {
      $config['receiveTimeoutInMilliseconds'] = static::RECEIVE_TIMEOUT_IN_MILLISECONDS;
    }

    $this->_receiveTimeoutInMilliseconds = $config['receiveTimeoutInMilliseconds'];

    if (!is_int($config['numberOfRetries']) || $config['numberOfRetries'] < 0) {
      $config['numberOfRetries'] = static::NUMBER_OF_RETRIES;
    }

    $this->_totalLoop = $config['numberOfRetries'] + 1;

    if (!is_int($config['scaleMode']) || ($config['scaleMode'] !== ScaleMode::BALANCING && $config['scaleMode'] !== ScaleMode::FAIL_OVER)) {
      $config['scaleMode'] = static::SCALE_MODE;
    }

    if ($config['scaleMode'] === ScaleMode::BALANCING) {
      $config['poolSelectOption'] = PoolSelectOption::ANY_ALIVE_OR_FIRST;
    } else {
      $config['poolSelectOption'] = PoolSelectOption::ALIVE_OR_FIRST;
    }

    $this->_poolSelectOption = $config['poolSelectOption'];
  }

  public function __destruct() {
  }

  private function _createTransportAndClient($host, $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->_sendTimeoutInMilliseconds);
    $socket->setRecvTimeout($this->_receiveTimeoutInMilliseconds);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneEventBusServiceClient($protocol);

    return [$transport, $client];
  }

  private function _getTransportAndClient() {
    $hostPorts = $this->_hostPorts;
    $hostPortsAliveStatus = $this->_hostPortsAliveStatus;
    $poolSize = count($hostPorts);
    $getFirstHostPort = false;

    if ($this->_poolSelectOption === PoolSelectOption::ANY_ALIVE_OR_FIRST) {
      $randomPoolIndex = rand(0, $poolSize - 1);

      for ($i = $randomPoolIndex; $i < $randomPoolIndex + $poolSize; ++$i) {
        $hostPort = $hostPorts[$i % $poolSize];

        if ($this->getHostPortAliveStatus($hostPortsAliveStatus, $hostPort) === false) {
          continue;
        }

        list($transport, $client) = $this->_createTransportAndClient($hostPort['host'], $hostPort['port']);

        if ($transport === null || $client === null) {
          continue;
        }

        return [$transport, $client, $hostPort];
      }

      $getFirstHostPort = true;
    } else if ($this->_poolSelectOption === PoolSelectOption::ALIVE_OR_FIRST) {
      for ($i = 0; $i < $poolSize; ++$i) {
        $hostPort = $hostPorts[$i];

        if ($this->getHostPortAliveStatus($hostPortsAliveStatus, $hostPort) === false) {
          continue;
        }

        list($transport, $client) = $this->_createTransportAndClient($hostPort['host'], $hostPort['port']);

        if ($transport === null || $client === null) {
          continue;
        }

        return [$transport, $client, $hostPort];
      }

      $getFirstHostPort = true;
    }

    if ($getFirstHostPort) {
      $hostPort = $hostPorts[0];

      list($transport, $client) = $this->_createTransportAndClient($hostPort['host'], $hostPort['port']);

      // Don't need to check null anymore

      return [$transport, $client, $hostPort];
    }

    throw new \Exception('Not supported yet');
  }

  public function getLastException() {
    return $this->lastException;
  }

  private function _createNotifyMessageBooleanRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->_topics)) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoBoolean();
    $messageInfo->clientId = $this->_clientId;
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

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->_topics)) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoInteger();
    $messageInfo->clientId = $this->_clientId;
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

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->_topics)) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoDouble();
    $messageInfo->clientId = $this->_clientId;
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

    if (!is_string($params['topic']) || empty($params['topic']) || !in_array($params['topic'], $this->_topics)) {
      throw new \Exception('Invalid "topic" param');
    }

    $messageInfo = new MessageInfoString();
    $messageInfo->clientId = $this->_clientId;
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

  /**
   * @return \Miczone\Thrift\Common\ErrorCode
   */
  public function ping() {
    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->ping($this->_operationHandle);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->notifyBoolean($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_notifyBoolean($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        if (is_callable($successCallback)) {
          call_user_func($successCallback);
        }

        return;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyInteger(array $params = []) {
    $request = $this->_createNotifyMessageIntegerRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->notifyInteger($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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
   * @throws \Exception
   */
  public function ow_notifyInteger(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageIntegerRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_notifyInteger($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        if (is_callable($successCallback)) {
          call_user_func($successCallback);
        }

        return;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyDouble(array $params = []) {
    $request = $this->_createNotifyMessageDoubleRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->notifyDouble($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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
   * @throws \Exception
   */
  public function ow_notifyDouble(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageDoubleRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_notifyDouble($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        if (is_callable($successCallback)) {
          call_user_func($successCallback);
        }

        return;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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
   * @return \Miczone\Thrift\EventBus\NotifyMessageResponse
   * @throws \Exception
   */
  public function notifyString(array $params = []) {
    $request = $this->_createNotifyMessageStringRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->notifyString($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        return $result;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
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
   * @throws \Exception
   */
  public function ow_notifyString(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createNotifyMessageStringRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_notifyString($this->_operationHandle, $request);
        $transport->close();

        $this->markHostPortAlive($this->_hostPortsAliveStatus, $hostPort);

        if (is_callable($successCallback)) {
          call_user_func($successCallback);
        }

        return;
      } catch (TTransportException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (TException $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      } catch (\Exception $ex) {
        $this->lastException = $ex;
        $this->markHostPortDead($this->_hostPortsAliveStatus, $hostPort);
        // Do something ...
      }
    }

    if (is_callable($errorCallback)) {
      call_user_func($errorCallback, $this->lastException);
    }
  }

}
