<?php

namespace Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByIdResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugResponse;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdListRequest;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdListResponse;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugListRequest;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugListResponse;
use Miczone\Thrift\Catalog\MiczoneCatalogStorageServiceClient;
use Miczone\Thrift\Catalog\Product\GetProductByIdRequest;
use Miczone\Thrift\Catalog\Product\GetProductByIdResponse;
use Miczone\Thrift\Catalog\Product\MultiGetProductByIdListRequest;
use Miczone\Thrift\Catalog\Product\MultiGetProductByIdListResponse;
use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Storage {
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

  private function _validateGetProductByIdRequest(GetProductByIdRequest $request) {
    if (!isset($request->id) || !is_string($request->id) || trim($request->id) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $request->id = trim($request->id);
  }

  private function _validateMultiGetProductByIdListRequest(MultiGetProductByIdListRequest $request) {
    if (!isset($request->idList) || !is_array($request->idList) || count($request->idList) === 0) {
      throw new \Exception('Invalid "idList" param');
    }
  }

  private function _validateGetCategoryByIdRequest(GetCategoryByIdRequest $request) {
    if (!isset($request->id) || !is_string($request->id) || trim($request->id) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $request->id = trim($request->id);
  }

  private function _validateGetCategoryBySlugRequest(GetCategoryBySlugRequest $request) {
    if (!isset($request->slug) || !is_string($request->slug) || trim($request->slug) === '') {
      throw new \Exception('Invalid "slug" param');
    }

    $request->slug = trim($request->slug);
  }

  private function _validateMultiGetCategoryByIdListRequest(MultiGetCategoryByIdListRequest $request) {
    if (!isset($request->idList) || !is_array($request->idList) || count($request->idList) === 0) {
      throw new \Exception('Invalid "idList" param');
    }
  }

  private function _validateMultiGetCategoryBySlugListRequest(MultiGetCategoryBySlugListRequest $request) {
    if (!isset($request->slugList) || !is_array($request->slugList) || count($request->slugList) === 0) {
      throw new \Exception('Invalid "slugList" param');
    }
  }

  private function _createTransportAndClient(string $host, int $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->config['receiveTimeoutInMilliseconds']);
    $socket->setRecvTimeout($this->config['sendTimeoutInMilliseconds']);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneCatalogStorageServiceClient($protocol);

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
   * @param \Miczone\Thrift\Catalog\Product\GetProductByIdRequest
   * @return \Miczone\Thrift\Catalog\Product\GetProductByIdResponse
   * @throws \Exception
   */
  public function getProductById(GetProductByIdRequest $request) {
    $this->_validateGetProductByIdRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getProductById($this->operationHandle, $request);
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

    return new GetProductByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Product\MultiGetProductByIdListRequest
   * @return \Miczone\Thrift\Catalog\Product\MultiGetProductByIdListResponse
   * @throws \Exception
   */
  public function multiGetProductByIdList(MultiGetProductByIdListRequest $request) {
    $this->_validateMultiGetProductByIdListRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->multiGetProductByIdList($this->operationHandle, $request);
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

    return new MultiGetProductByIdListResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Product\GetCategoryByIdRequest
   * @return \Miczone\Thrift\Catalog\Product\GetCategoryByIdResponse
   * @throws \Exception
   */
  public function getCategoryById(GetCategoryByIdRequest $request) {
    $this->_validateGetCategoryByIdRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getCategoryById($this->operationHandle, $request);
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

    return new GetCategoryByIdResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryBySlugResponse
   * @throws \Exception
   */
  public function getCategoryBySlug(GetCategoryBySlugRequest $request) {
    $this->_validateGetCategoryBySlugRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getCategoryBySlug($this->operationHandle, $request);
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

    return new GetCategoryBySlugResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdListRequest
   * @return \Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdListResponse
   * @throws \Exception
   */
  public function multiGetCategoryByIdList(MultiGetCategoryByIdListRequest $request) {
    $this->_validateMultiGetCategoryByIdListRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->multiGetCategoryByIdList($this->operationHandle, $request);
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

    return new MultiGetCategoryByIdListResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugListRequest
   * @return \Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugListResponse
   * @throws \Exception
   */
  public function multiGetCategoryBySlugList(MultiGetCategoryBySlugListRequest $request) {
    $this->_validateMultiGetCategoryBySlugListRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->multiGetCategoryBySlugList($this->operationHandle, $request);
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

    return new MultiGetCategoryBySlugListResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

}
