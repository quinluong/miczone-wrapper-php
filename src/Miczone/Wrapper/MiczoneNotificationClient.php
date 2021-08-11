<?php

namespace Miczone\Wrapper;

use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Miczone\Thrift\Common\PoolSelectOption;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Thrift\Notification\DeleteNotificationItemRequest;
use Miczone\Thrift\Notification\DeleteNotificationItemResponse;
use Miczone\Thrift\Notification\GetNotificationCounterRequest;
use Miczone\Thrift\Notification\GetNotificationCounterResponse;
use Miczone\Thrift\Notification\GetNotificationItemListRequest;
use Miczone\Thrift\Notification\GetNotificationItemListResponse;
use Miczone\Thrift\Notification\InsertNotificationItemRequest;
use Miczone\Thrift\Notification\InsertNotificationItemResponse;
use Miczone\Thrift\Notification\MiczoneNotificationServiceClient;
use Miczone\Thrift\Notification\NotificationItemTargetUserType;
use Miczone\Thrift\Notification\ResetNotificationCounterRequest;
use Miczone\Thrift\Notification\ResetNotificationCounterResponse;
use Miczone\Thrift\Notification\UpdateNotificationItemRequest;
use Miczone\Thrift\Notification\UpdateNotificationItemResponse;
use Miczone\Thrift\Notification\UpdateReadNotificationItemRequest;
use Miczone\Thrift\Notification\UpdateReadNotificationItemResponse;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class MiczoneNotificationClient extends MiczoneClientBase {

  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const AUTH = '';

  const SEND_TIMEOUT_IN_MILLISECONDS = 1000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 2000;

  const NUMBER_OF_RETRIES = 1;

  const SCALE_MODE = ScaleMode::BALANCING;

  protected $lastException;

  private $_hostPorts;

  private $_hostPortsAliveStatus;

  private $_operationHandle;

  private $_sendTimeoutInMilliseconds;

  private $_receiveTimeoutInMilliseconds;

  private $_totalLoop;

  private $_poolSelectOption;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'auth' => static::AUTH,
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

    $config['auth'] = $this->standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
    }

    $this->_operationHandle = new OperationHandle([
      'username' => $config['auth']['username'],
      'password' => $config['auth']['password'],
    ]);

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

  private function _createTransportAndClient(string $host, int $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->_sendTimeoutInMilliseconds);
    $socket->setRecvTimeout($this->_receiveTimeoutInMilliseconds);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneNotificationServiceClient($protocol);

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

  private function _createGetCounterRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['userId']) || !is_int($params['userId']) || $params['userId'] <= 0) {
      throw new \Exception('Invalid "userId" param');
    }

    $intUserId = $params['userId'];

    $request = new GetNotificationCounterRequest([
      'publisher' => $strPublisher,
      'userId' => $intUserId,
    ]);

    return $request;
  }

  private function _createResetCounterRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['userId']) || !is_int($params['userId']) || $params['userId'] <= 0) {
      throw new \Exception('Invalid "userId" param');
    }

    $intUserId = $params['userId'];

    $request = new ResetNotificationCounterRequest([
      'publisher' => $strPublisher,
      'userId' => $intUserId,
    ]);

    return $request;
  }

  private function _createGetItemListRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['userId']) || !is_int($params['userId']) || $params['userId'] < 0) {
      throw new \Exception('Invalid "userId" param');
    }

    $intUserId = $params['userId'];

    $request = new GetNotificationItemListRequest([
      'publisher' => $strPublisher,
      'userId' => $intUserId,
    ]);

    if (isset($params['groupId']) && is_int($params['groupId']) && $params['groupId'] >= 0) {
      $request->groupId = $params['groupId'];
    }

    if (isset($params['fromItemId']) && is_int($params['fromItemId']) && $params['fromItemId'] > 0) {
      $request->fromItemId = $params['fromItemId'];
    }

    if (isset($params['limit']) && is_int($params['limit']) && $params['limit'] > 0) {
      $request->limit = $params['limit'];
    }

    return $request;
  }

  private function _createInsertItemRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['targetUserType']) || !is_int($params['targetUserType']) || !array_key_exists($params['targetUserType'], NotificationItemTargetUserType::$__names)) {
      throw new \Exception('Invalid "targetUserType" param');
    }

    $intTargetUserType = $params['targetUserType'];

    if (!isset($params['id']) || !is_int($params['id']) || $params['id'] <= 0) {
      throw new \Exception('Invalid "id" param');
    }

    $intId = $params['id'];

    if (!isset($params['groupId']) || !is_int($params['groupId']) || $params['groupId'] < 0) {
      throw new \Exception('Invalid "groupId" param');
    }

    $intGroupId = $params['groupId'];

    if (!isset($params['typeId']) || !is_int($params['typeId']) || $params['typeId'] < 0) {
      throw new \Exception('Invalid "typeId" param');
    }

    $intTypeId = $params['typeId'];

    if (!isset($params['title']) || !is_string($params['title']) || trim($params['title']) === '') {
      throw new \Exception('Invalid "title" param');
    }

    $strTitle = trim($params['title']);

    $request = new InsertNotificationItemRequest([
      'publisher' => $strPublisher,
      'targetUserType' => $intTargetUserType,
      'id' => $intId,
      'groupId' => $intGroupId,
      'typeId' => $intTypeId,
      'title' => $strTitle,
    ]);

    if (isset($params['registeredUserIdSet']) && is_array($params['registeredUserIdSet']) && count($params['registeredUserIdSet']) > 0) {
      $registeredUserIdSet = [];

      foreach ($params['registeredUserIdSet'] as $registeredUserId) {
        $registeredUserIdSet[$registeredUserId] = true;
      }

      $request->registeredUserIdSet = $registeredUserIdSet;
    }

    if (isset($params['content']) && is_string($params['content']) && trim($params['content']) !== '') {
      $request->content = trim($params['content']);
    }

    if (isset($params['url']) && is_string($params['url']) && trim($params['url']) !== '') {
      $request->url = trim($params['url']);

      if (isset($params['isTargetBlank']) && is_bool($params['isTargetBlank'])) {
        $request->isTargetBlank = $params['isTargetBlank'];
      }
    }

    if (isset($params['imageUrl']) && is_string($params['imageUrl']) && trim($params['imageUrl']) !== '') {
      $request->imageUrl = trim($params['imageUrl']);
    }

    return $request;
  }

  private function _createUpdateItemRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['id']) || !is_int($params['id']) || $params['id'] <= 0) {
      throw new \Exception('Invalid "id" param');
    }

    $intId = $params['id'];

    $intTypeId = -1;

    if (isset($params['typeId']) && is_int($params['typeId']) && $params['typeId'] >= 0) {
      $intTypeId = $params['typeId'];
    }

    $strTitle = null;

    if (isset($params['title']) && is_string($params['title']) && trim($params['title']) !== '') {
      $strTitle = trim($params['title']);
    }

    $strContent = null;

    if (isset($params['content']) && is_string($params['content'])) {
      $strContent = trim($params['content']);
    }

    $strUrl = null;

    if (isset($params['url']) && is_string($params['url'])) {
      $strUrl = trim($params['url']);
    }

    $isTargetBlank = null;

    if (isset($params['isTargetBlank']) && is_bool($params['isTargetBlank'])) {
      $isTargetBlank = $params['isTargetBlank'];
    }

    $strImageUrl = null;

    if (isset($params['imageUrl']) && is_string($params['imageUrl'])) {
      $strImageUrl = trim($params['imageUrl']);
    }

    if ($intTypeId < 0 && $strTitle === null && $strContent === null && $strUrl === null && $isTargetBlank === null && $strImageUrl === null) {
      throw new \Exception('Invalid params');
    }

    $request = new UpdateNotificationItemRequest([
      'publisher' => $strPublisher,
      'id' => $intId,
    ]);

    if ($intTypeId >= 0) {
      $request->typeId = $intTypeId;
    }

    if ($strTitle !== null) {
      $request->title = $strTitle;
    }

    if ($strContent !== null) {
      $request->content = $strContent;
    }

    if ($strUrl !== null) {
      $request->url = $strUrl;
    }

    if ($isTargetBlank !== null) {
      $request->isTargetBlank = $isTargetBlank;
    }

    if ($strImageUrl !== null) {
      $request->imageUrl = $strImageUrl;
    }

    return $request;
  }

  private function _createUpdateReadItemRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['userId']) || !is_int($params['userId']) || $params['userId'] <= 0) {
      throw new \Exception('Invalid "userId" param');
    }

    $intUserId = $params['userId'];

    if (!isset($params['id']) || !is_int($params['id']) || $params['id'] <= 0) {
      throw new \Exception('Invalid "id" param');
    }

    $intId = $params['id'];

    $isRead = true;

    if (isset($params['isRead']) && is_bool($params['isRead'])) {
      $isRead = $params['isRead'];
    }

    $request = new UpdateReadNotificationItemRequest([
      'publisher' => $strPublisher,
      'userId' => $intUserId,
      'id' => $intId,
      'isRead' => $isRead,
    ]);

    return $request;
  }

  private function _createDeleteItemRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['publisher']) || !is_string($params['publisher']) || trim($params['publisher']) === '') {
      throw new \Exception('Invalid "publisher" param');
    }

    $strPublisher = trim($params['publisher']);

    if (!isset($params['id']) || !is_int($params['id']) || $params['id'] <= 0) {
      throw new \Exception('Invalid "id" param');
    }

    $intId = $params['id'];

    $request = new DeleteNotificationItemRequest([
      'publisher' => $strPublisher,
      'id' => $intId,
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
   * string   publisher (required)
   * int      userId (required)
   * @return \Miczone\Thrift\Notification\GetNotificationCounterResponse
   * @throws \Exception
   */
  public function getCounter(array $params) {
    $request = $this->_createGetCounterRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCounter($this->_operationHandle, $request);
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

    return new GetNotificationCounterResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      userId (required)
   * @return \Miczone\Thrift\Notification\ResetNotificationCounterResponse
   * @throws \Exception
   */
  public function resetCounter(array $params) {
    $request = $this->_createResetCounterRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->resetCounter($this->_operationHandle, $request);
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

    return new ResetNotificationCounterResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      userId (required)
   * array    $successCallbackUrlList
   * array    $errorCallbackUrlList
   * @throws \Exception
   */
  public function ow_resetCounter(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createResetCounterRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_resetCounter($this->_operationHandle, $request);
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
   * string   publisher (required)
   * int      userId (required)
   * int      groupId
   * int      fromItemId
   * int      limit
   * @return \Miczone\Thrift\Notification\GetNotificationItemListResponse
   * @throws \Exception
   */
  public function getItemList(array $params) {
    $request = $this->_createGetItemListRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getItemList($this->_operationHandle, $request);
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

    return new GetNotificationItemListResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      targetUserType (required)
   * array    registeredUserIdSet (required if targetUserType = REGISTERED)
   * int      id (required)
   * int      groupId (required)
   * int      typeId (required)
   * string   title (required)
   * string   content
   * string   url
   * bool     isTargetBlank
   * string   imageUrl
   * @return \Miczone\Thrift\Notification\InsertNotificationItemResponse
   * @throws \Exception
   */
  public function insertItem(array $params) {
    $request = $this->_createInsertItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->insertItem($this->_operationHandle, $request);
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

    return new InsertNotificationItemResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      targetUserType (required)
   * array    registeredUserIdSet (required if targetUserType = REGISTERED)
   * int      id (required)
   * int      groupId (required)
   * int      typeId (required)
   * string   title (required)
   * string   content
   * string   url
   * bool     isTargetBlank
   * string   imageUrl
   * array    $successCallbackUrlList
   * array    $errorCallbackUrlList
   * @throws \Exception
   */
  public function ow_insertItem(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createInsertItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_insertItem($this->_operationHandle, $request);
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
   * string   publisher (required)
   * int      id (required)
   * int      typeId
   * string   title
   * string   content
   * string   url
   * bool     isTargetBlank
   * string   imageUrl
   * @return \Miczone\Thrift\Notification\UpdateNotificationItemResponse
   * @throws \Exception
   */
  public function updateItem(array $params) {
    $request = $this->_createUpdateItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->updateItem($this->_operationHandle, $request);
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

    return new UpdateNotificationItemResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      id (required)
   * int      typeId
   * string   title
   * string   content
   * string   url
   * bool     isTargetBlank
   * string   imageUrl
   * array    $successCallbackUrlList
   * array    $errorCallbackUrlList
   * @throws \Exception
   */
  public function ow_updateItem(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createUpdateItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_updateItem($this->_operationHandle, $request);
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
   * string   publisher (required)
   * int      userId (required)
   * int      id (required)
   * bool     isRead (required)
   * @return \Miczone\Thrift\Notification\UpdateReadNotificationItemResponse
   * @throws \Exception
   */
  public function updateReadItem(array $params) {
    $request = $this->_createUpdateReadItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->updateReadItem($this->_operationHandle, $request);
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

    return new UpdateReadNotificationItemResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      userId (required)
   * int      id (required)
   * bool     isRead (required)
   * array    $successCallbackUrlList
   * array    $errorCallbackUrlList
   * @throws \Exception
   */
  public function ow_updateReadItem(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createUpdateReadItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_updateReadItem($this->_operationHandle, $request);
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
   * string   publisher (required)
   * int      id (required)
   * @return \Miczone\Thrift\Notification\DeleteNotificationItemResponse
   * @throws \Exception
   */
  public function deleteItem(array $params) {
    $request = $this->_createDeleteItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->deleteItem($this->_operationHandle, $request);
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

    return new DeleteNotificationItemResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      id (required)
   * array    $successCallbackUrlList
   * array    $errorCallbackUrlList
   * @throws \Exception
   */
  public function ow_deleteItem(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createDeleteItemRequest($params);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $client->ow_deleteItem($this->_operationHandle, $request);
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
