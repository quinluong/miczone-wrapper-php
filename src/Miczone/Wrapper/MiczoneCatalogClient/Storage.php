<?php

namespace Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\GetCategoryByIdRequest;
use Miczone\Thrift\Catalog\GetCategoryByIdResponse;
use Miczone\Thrift\Catalog\GetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\GetCategoryBySlugResponse;
use Miczone\Thrift\Catalog\GetProductByIdRequest;
use Miczone\Thrift\Catalog\GetProductByIdResponse;
use Miczone\Thrift\Catalog\GetProductBySkuRequest;
use Miczone\Thrift\Catalog\GetProductBySkuResponse;
use Miczone\Thrift\Catalog\MiczoneCatalogStorageServiceClient;
use Miczone\Thrift\Catalog\MultiGetCategoryByIdListRequest;
use Miczone\Thrift\Catalog\MultiGetCategoryByIdListResponse;
use Miczone\Thrift\Catalog\MultiGetCategoryBySlugListRequest;
use Miczone\Thrift\Catalog\MultiGetCategoryBySlugListResponse;
use Miczone\Thrift\Catalog\MultiGetProductByIdListRequest;
use Miczone\Thrift\Catalog\MultiGetProductByIdListResponse;
use Miczone\Thrift\Catalog\MultiGetProductBySkuListRequest;
use Miczone\Thrift\Catalog\MultiGetProductBySkuListResponse;
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

  private function _createGetProductByIdRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['id']) || !is_string($params['id']) || trim($params['id']) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $strId = trim($params['id']);

    $request = new GetProductByIdRequest([
      'id' => $strId,
    ]);

    return $request;
  }

  private function _createGetProductBySkuRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['sku']) || !is_string($params['sku']) || trim($params['sku']) === '') {
      throw new \Exception('Invalid "sku" param');
    }

    $strSku = trim($params['sku']);

    $request = new GetProductBySkuRequest([
      'sku' => $strSku,
    ]);

    return $request;
  }

  private function _createMultiGetProductByIdListRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['idList']) || !is_array($params['idList']) || count($params['idList']) === 0) {
      throw new \Exception('Invalid "idList" param');
    }

    $arrId = trim($params['idList']);

    $request = new MultiGetProductByIdListRequest([
      'idList' => $arrId,
    ]);

    return $request;
  }

  private function _createMultiGetProductBySkuListRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['skuList']) || !is_array($params['skuList']) || count($params['skuList']) === 0) {
      throw new \Exception('Invalid "skuList" param');
    }

    $arrSku = trim($params['skuList']);

    $request = new MultiGetProductBySkuListRequest([
      'skuList' => $arrSku,
    ]);

    return $request;
  }

  private function _createGetCategoryByIdRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['id']) || !is_string($params['id']) || trim($params['id']) === '') {
      throw new \Exception('Invalid "id" param');
    }

    $strId = trim($params['id']);

    $request = new GetCategoryByIdRequest([
      'id' => $strId,
    ]);

    return $request;
  }

  private function _createGetCategoryBySlugRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['slug']) || !is_string($params['slug']) || trim($params['slug']) === '') {
      throw new \Exception('Invalid "slug" param');
    }

    $strSlug = trim($params['slug']);

    $request = new GetCategoryBySlugRequest([
      'slug' => $strSlug,
    ]);

    return $request;
  }

  private function _createMultiGetCategoryByIdListRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['idList']) || !is_array($params['idList']) || count($params['idList']) === 0) {
      throw new \Exception('Invalid "idList" param');
    }

    $arrId = trim($params['idList']);

    $request = new MultiGetCategoryByIdListRequest([
      'idList' => $arrId,
    ]);

    return $request;
  }

  private function _createMultiGetCategoryBySlugListRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!isset($params['slugList']) || !is_array($params['slugList']) || count($params['slugList']) === 0) {
      throw new \Exception('Invalid "slugList" param');
    }

    $arrSlug = trim($params['slugList']);

    $request = new MultiGetCategoryBySlugListRequest([
      'slugList' => $arrSlug,
    ]);

    return $request;
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
   * @param array $params Contains:
   * string     id (required)
   * @return \Miczone\Thrift\Catalog\GetProductByIdResponse
   * @throws \Exception
   */
  public function getProductById(array $params) {
    $request = $this->_createGetProductByIdRequest($params);

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
   * @param array $params Contains:
   * string     sku (required)
   * @return \Miczone\Thrift\Catalog\GetProductBySkuResponse
   * @throws \Exception
   */
  public function getProductBySku(array $params) {
    $request = $this->_createGetProductBySkuRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getProductBySku($this->operationHandle, $request);
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

    return new GetProductBySkuResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * array      idList (required)
   * @return \Miczone\Thrift\Catalog\MultiGetProductByIdListResponse
   * @throws \Exception
   */
  public function multiGetProductByIdList(array $params) {
    $request = $this->_createMultiGetProductByIdListRequest($params);

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
   * @param array $params Contains:
   * array      skuList (required)
   * @return \Miczone\Thrift\Catalog\MultiGetProductBySkuListResponse
   * @throws \Exception
   */
  public function multiGetProductBySkuList(array $params) {
    $request = $this->_createMultiGetProductBySkuListRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->multiGetProductBySkuList($this->operationHandle, $request);
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

    return new MultiGetProductBySkuListResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * string     id (required)
   * @return \Miczone\Thrift\Catalog\GetCategoryByIdResponse
   * @throws \Exception
   */
  public function getCategoryById(array $params) {
    $request = $this->_createGetCategoryByIdRequest($params);

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
   * @param array $params Contains:
   * string     slug (required)
   * @return \Miczone\Thrift\Catalog\GetCategoryBySlugResponse
   * @throws \Exception
   */
  public function getCategoryBySlug(array $params) {
    $request = $this->_createGetCategoryBySlugRequest($params);

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
   * @param array $params Contains:
   * array      idList (required)
   * @return \Miczone\Thrift\Catalog\MultiGetCategoryByIdListResponse
   * @throws \Exception
   */
  public function multiGetCategoryByIdList(array $params) {
    $request = $this->_createMultiGetCategoryByIdListRequest($params);

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
   * @param array $params Contains:
   * array      slugList (required)
   * @return \Miczone\Thrift\Catalog\MultiGetCategoryBySlugListResponse
   * @throws \Exception
   */
  public function multiGetCategoryBySlugList(array $params) {
    $request = $this->_createMultiGetCategoryBySlugListRequest($params);

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
