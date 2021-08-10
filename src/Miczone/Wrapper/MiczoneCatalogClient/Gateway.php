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
use Miczone\Thrift\Common\PoolSelectOption;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Wrapper\MiczoneClientBase;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Gateway extends MiczoneClientBase {

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

    if (!is_int($config['numberOfRetries']) || $config['numberOfRetries'] <= 0) {
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

    $client = new MiczoneCatalogGatewayServiceClient($protocol);

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

  public function setTraceId(string $value) {
    if ($value === null || trim($value) === '') {
      throw new \Exception('Invalid "value" param');
    }

    $this->_operationHandle->traceId = trim($value);
  }

  public function getLastException() {
    return $this->lastException;
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

  private function _validateMultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest(MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest $request) {
    if (!isset($request->dataMap) || !is_array($request->dataMap) || count($request->dataMap) === 0) {
      throw new \Exception('Invalid "dataMap" param');
    }
  }

  private function _validateGetMatrixProductRequest(GetMatrixProductRequest $request) {
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
   * @param \Miczone\Thrift\Catalog\Search\SearchProductRequest
   * @return \Miczone\Thrift\Catalog\Search\SearchProductResponse
   * @throws \Exception
   */
  public function searchProduct(SearchProductRequest $request) {
    $this->_validateSearchProductRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->searchProduct($this->_operationHandle, $request);
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

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryById($this->_operationHandle, $request);
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

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryBySlug($this->_operationHandle, $request);
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

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryByOriginalCategory($this->_operationHandle, $request);
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

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getCategoryByProductSkuAndOriginalMerchant($this->_operationHandle, $request);
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
    $this->_validateMultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->multiGetBreadcrumbListByProductSkuAndOriginalMerchant($this->_operationHandle, $request);
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
    $this->_validateGetMatrixProductRequest($request);

    for ($i = 0; $i < $this->_totalLoop; ++$i) {
      list($transport, $client, $hostPort) = $this->_getTransportAndClient();

      if ($transport === null || $client === null) {
        // Do something ...
        continue;
      }

      try {
        $transport->open();
        $result = $client->getMatrixProduct($this->_operationHandle, $request);
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

    return new GetMatrixProductResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

}
