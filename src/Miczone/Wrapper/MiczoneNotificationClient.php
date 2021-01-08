<?php

namespace Miczone\Wrapper;

use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
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

class MiczoneNotificationClient {
  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const AUTH = '';

  const SEND_TIMEOUT_IN_MILLISECONDS = 5000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 5000;

  const NUMBER_OF_RETRIES = 3;

  protected $config;

  protected $operationHandle;

  protected $lastException;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'auth' => static::AUTH,
      'sendTimeoutInMilliseconds' => static::SEND_TIMEOUT_IN_MILLISECONDS,
      'receiveTimeoutInMilliseconds' => static::RECEIVE_TIMEOUT_IN_MILLISECONDS,
      'numberOfRetries' => static::NUMBER_OF_RETRIES,
    ], $config);

    $config['hosts'] = $this->_standardizeHosts($config['hosts']);

    if (empty($config['hosts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    $config['auth'] = $this->_standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
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
      \array_push($result, [
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
      $request->registeredUserIdSet = $params['registeredUserIdSet'];
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

  private function _createTransportAndClient(string $host, int $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->config['receiveTimeoutInMilliseconds']);
    $socket->setRecvTimeout($this->config['sendTimeoutInMilliseconds']);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneNotificationServiceClient($protocol);

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
          $result = $client->ping($this->operationHandle);
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
   * string   publisher (required)
   * int      userId (required)
   * @return \Miczone\Thrift\Notification\GetNotificationCounterResponse
   * @throws \Exception
   */
  public function getCounter(array $params) {
    $request = $this->_createGetCounterRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getCounter($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->resetCounter($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_resetCounter($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getItemList($this->operationHandle, $request);
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

    return new GetNotificationItemListResponse([
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
   * @return \Miczone\Thrift\Notification\UpdateReadNotificationItemResponse
   * @throws \Exception
   */
  public function updateReadItem(array $params) {
    $request = $this->_createUpdateReadItemRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->updateReadItem($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_updateReadItem($this->operationHandle, $request);
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
   * string   publisher (required)
   * int      id (required)
   * array    userIdSet (required)
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->insertItem($this->operationHandle, $request);
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

    return new InsertNotificationItemResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string   publisher (required)
   * int      id (required)
   * array    userIdSet (required)
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_insertItem($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->updateItem($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_updateItem($this->operationHandle, $request);
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
   * string   publisher (required)
   * int      id (required)
   * @return \Miczone\Thrift\Notification\DeleteNotificationItemResponse
   * @throws \Exception
   */
  public function deleteItem(array $params) {
    $request = $this->_createDeleteItemRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->deleteItem($this->operationHandle, $request);
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

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_deleteItem($this->operationHandle, $request);
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
