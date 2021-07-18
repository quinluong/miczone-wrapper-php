<?php

namespace Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\Breadcrumb\MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest;
use Miczone\Thrift\Catalog\Breadcrumb\MultiGetBreadcrumbListByProductSkuAndOriginalMerchantResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByIdResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryByOriginalCategoryRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByOriginalCategoryResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryByProductSkuAndOriginalMerchantRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByProductSkuAndOriginalMerchantResponse;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugResponse;
use Miczone\Thrift\Catalog\MiczoneCatalogGatewayServiceClient;
use Miczone\Thrift\Catalog\Product\GetMatrixProductRequest;
use Miczone\Thrift\Catalog\Product\GetMatrixProductResponse;
use Miczone\Thrift\Catalog\Search\SearchProductRequest;
use Miczone\Thrift\Catalog\Search\SearchProductResponse;
use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Miczone\Thrift\Common\ScaleMode;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Gateway {
  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const AUTH = '';

  const SEND_TIMEOUT_IN_MILLISECONDS = 5000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 5000;

  const NUMBER_OF_RETRIES = 3;

  const SCALE_MODE = ScaleMode::BALANCING;

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
      'scaleMode' => static::SCALE_MODE,
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

  private function _validateSearchProductRequest(SearchProductRequest $request) {
    if (isset($request->sortBy)
      && (!is_string($request->sortBy) || trim($request->sortBy) === '')) {
      throw new \Exception('Invalid "sortBy" param');
    }

    if (isset($request->productCount)
      && (!is_int($request->productCount) || $request->productCount <= 0)) {
      throw new \Exception('Invalid "productCount" param');
    }

    if (isset($request->productPage)
      && (!is_int($request->productPage) || $request->productPage <= 0)) {
      throw new \Exception('Invalid "productPage" param');
    }

    if (isset($request->keyword)
      && (!is_string($request->keyword) || trim($request->keyword) === '')) {
      throw new \Exception('Invalid "keyword" param');
    }

    if (isset($request->categoryIdList)
      && (!is_array($request->categoryIdList) || count($request->categoryIdList) === 0)) {
      throw new \Exception('Invalid "categoryIdList" param');
    }

    if (isset($request->minPrice)
      && (!is_float($request->minPrice) || $request->minPrice < 0)) {
      throw new \Exception('Invalid "minPrice" param');
    }

    if (isset($request->maxPrice)
      && (!is_float($request->maxPrice) || $request->maxPrice < 0)) {
      throw new \Exception('Invalid "maxPrice" param');
    }

    if (isset($request->filterGroupList)
      && (!is_array($request->filterGroupList) || count($request->filterGroupList) === 0)) {
      throw new \Exception('Invalid "filterGroupList" param');
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

  private function _validateGetCategoryByOriginalCategoryRequest(GetCategoryByOriginalCategoryRequest $request) {
    if (!isset($request->originalCategoryOriginalId) || !is_string($request->originalCategoryOriginalId) || trim($request->originalCategoryOriginalId) === '') {
      throw new \Exception('Invalid "originalCategoryOriginalId" param');
    }

    $request->originalCategoryOriginalId = trim($request->originalCategoryOriginalId);
  }

  private function _validateGetCategoryByProductSkuAndOriginalMerchantRequest(GetCategoryByProductSkuAndOriginalMerchantRequest $request) {
    if (!isset($request->productSku) || !is_string($request->productSku) || trim($request->productSku) === '') {
      throw new \Exception('Invalid "productSku" param');
    }

    $request->productSku = trim($request->productSku);

    if (!isset($request->originalMerchantOriginalId) || !is_string($request->originalMerchantOriginalId) || trim($request->originalMerchantOriginalId) === '') {
      throw new \Exception('Invalid "originalMerchantOriginalId" param');
    }

    $request->originalMerchantOriginalId = trim($request->originalMerchantOriginalId);
  }

  private function _validateMultiGetBreadcrumbListByProductSkuAndOriginalMerchant(MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest $request) {
    if (!isset($request->dataMap) || !is_array($request->dataMap) || count($request->dataMap) === 0) {
      throw new \Exception('Invalid "dataMap" param');
    }
  }

  private function _validateGetMatrixProduct(GetMatrixProductRequest $request) {
    if (!isset($request->websiteCode) || !is_string($request->websiteCode) || trim($request->websiteCode) === '') {
      throw new \Exception('Invalid "websiteCode" param');
    }

    $request->websiteCode = trim($request->websiteCode);

    if (!isset($request->countryCode) || !is_string($request->countryCode) || trim($request->countryCode) === '') {
      throw new \Exception('Invalid "countryCode" param');
    }

    $request->countryCode = trim($request->countryCode);

    if (!isset($request->productSku) || !is_string($request->productSku) || trim($request->productSku) === '') {
      throw new \Exception('Invalid "productSku" param');
    }

    $request->productSku = trim($request->productSku);

    if (!isset($request->originalMerchantOriginalId) || !is_string($request->originalMerchantOriginalId) || trim($request->originalMerchantOriginalId) === '') {
      throw new \Exception('Invalid "originalMerchantOriginalId" param');
    }

    $request->originalMerchantOriginalId = trim($request->originalMerchantOriginalId);
  }

  private function _createTransportAndClient(string $host, int $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->config['sendTimeoutInMilliseconds']);
    $socket->setRecvTimeout($this->config['receiveTimeoutInMilliseconds']);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneCatalogGatewayServiceClient($protocol);

    return [$transport, $client];
  }

  private function _getTransportAndClient() {
    $poolSize = count($this->config['hosts']);

    if ($this->config['scaleMode'] === ScaleMode::BALANCING) {
      $randomPoolIndex = rand(0, $poolSize - 1);

      for ($i = $randomPoolIndex; $i < $randomPoolIndex + $poolSize; ++$i) {
        $hostPortPair = $this->config['hosts'][$i % $poolSize];

        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          continue;
        }

        return [$transport, $client];
      }

      // Get first host-port
      $hostPortPair = $this->config['hosts'][0];

      return $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);
    } else {
      for ($i = 0; $i < $poolSize; ++$i) {
        $hostPortPair = $this->config['hosts'][$i];

        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          continue;
        }

        return [$transport, $client];
      }

      // Get first host-port
      $hostPortPair = $this->config['hosts'][0];

      return $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);
    }

    throw new \Exception('Not supported yet');
  }

  public function setTraceId(string $value) {
    if ($value === null || trim($value) === '') {
      throw new \Exception('Invalid "value" param');
    }

    $this->operationHandle->traceId = trim($value);
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
   * @param \Miczone\Thrift\Catalog\Search\SearchProductRequest
   * @return \Miczone\Thrift\Catalog\Search\SearchProductResponse
   * @throws \Exception
   */
  public function searchProduct(SearchProductRequest $request) {
    $this->_validateSearchProductRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->searchProduct($this->operationHandle, $request);
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

    return new SearchProductResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryByIdResponse
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
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryByOriginalCategoryRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryByOriginalCategoryResponse
   * @throws \Exception
   */
  public function getCategoryByOriginalCategory(GetCategoryByOriginalCategoryRequest $request) {
    $this->_validateGetCategoryByOriginalCategoryRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getCategoryByOriginalCategory($this->operationHandle, $request);
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

    return new GetCategoryByOriginalCategoryResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Category\GetCategoryByProductSkuAndOriginalMerchantRequest
   * @return \Miczone\Thrift\Catalog\Category\GetCategoryByProductSkuAndOriginalMerchantResponse
   * @throws \Exception
   */
  public function getCategoryByProductSkuAndOriginalMerchant(GetCategoryByProductSkuAndOriginalMerchantRequest $request) {
    $this->_validateGetCategoryByProductSkuAndOriginalMerchantRequest($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getCategoryByProductSkuAndOriginalMerchant($this->operationHandle, $request);
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

    return new GetCategoryByProductSkuAndOriginalMerchantResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Breadcrumb\MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest
   * @return \Miczone\Thrift\Catalog\Breadcrumb\MultiGetBreadcrumbListByProductSkuAndOriginalMerchantResponse
   * @throws \Exception
   */
  public function multiGetBreadcrumbListByProductSkuAndOriginalMerchant(MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest $request) {
    $this->_validateMultiGetBreadcrumbListByProductSkuAndOriginalMerchant($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->multiGetBreadcrumbListByProductSkuAndOriginalMerchant($this->operationHandle, $request);
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

    return new MultiGetBreadcrumbListByProductSkuAndOriginalMerchantResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param \Miczone\Thrift\Catalog\Product\GetMatrixProductRequest
   * @return \Miczone\Thrift\Catalog\Product\GetMatrixProductResponse
   * @throws \Exception
   */
  public function getMatrixProduct(GetMatrixProductRequest $request) {
    $this->_validateGetMatrixProduct($request);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getMatrixProduct($this->operationHandle, $request);
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

    return new GetMatrixProductResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

}
