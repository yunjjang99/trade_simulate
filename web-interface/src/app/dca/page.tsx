'use client';

import { useState, useCallback } from 'react';
import {
  ArrowTrendingUpIcon,
  PlayIcon,
  ExclamationTriangleIcon,
  ChartBarIcon,
  DocumentTextIcon,
  CurrencyDollarIcon
} from '@heroicons/react/24/outline';
import { SimulationResult, parseCSVData, DCAStrategyParams } from '@/lib/simulation';
import DataFileSelector from '@/components/DataFileSelector';
import LoadingModal from '@/components/LoadingModal';
import EquityCurve from '@/components/EquityCurve';

export default function DCAPage() {
  const [selectedFile, setSelectedFile] = useState<string>('');
  const [params, setParams] = useState<DCAStrategyParams>({
    leverage: 20,
    roi_net: 0.05,
    fee_maker: 0.0002,
    init_margin: 2000,
    add_margin: 800,
    drops: [0.01, 0.02, 0.03, 0.04],
    mmr: 0.005
  });
  const [result, setResult] = useState<SimulationResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string>('');
  const [validationErrors, setValidationErrors] = useState<{ [key: string]: string }>({});

  const loadData = useCallback(async (filename: string) => {
    try {
      const response = await fetch(`/data/${filename}`);
      const csvText = await response.text();
      const parsedData = parseCSVData(csvText);
      return parsedData;
    } catch (error) {
      console.error('Failed to load data:', error);
      throw error;
    }
  }, []);

  const runSimulation = async () => {
    if (!selectedFile) {
      setError('데이터 파일을 선택해주세요');
      return;
    }

    if (!isFormValid) {
      setError('모든 파라미터를 올바르게 입력해주세요');
      return;
    }

    setLoading(true);
    setProgress(0);
    setError('');

    try {
      const simulationData = await loadData(selectedFile);

      const worker = new Worker(new URL('@/lib/worker.ts', import.meta.url));

      worker.postMessage({
        data: simulationData,
        strategyType: 'dca',
        scenario: 'all', // DCA는 시나리오 구분 없음
        params: params
      });

      worker.onmessage = (e) => {
        const { type, result, progress: workerProgress, error: workerError } = e.data;

        if (type === 'progress') {
          setProgress(workerProgress);
        } else if (type === 'success') {
          setResult(result);
          setProgress(100);
          setTimeout(() => {
            setLoading(false);
          }, 500);
          worker.terminate();
        } else if (type === 'error') {
          setError(workerError);
          setLoading(false);
          setProgress(0);
          worker.terminate();
        }
      };

      worker.onerror = () => {
        setError('시뮬레이션 중 오류가 발생했습니다');
        setLoading(false);
        setProgress(0);
        worker.terminate();
      };

    } catch (error: unknown) {
      setError(error instanceof Error ? error.message : '시뮬레이션 실행 실패');
      setLoading(false);
      setProgress(0);
    }
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(value);
  };

  const formatPercentage = (value: number) => {
    return `${(value * 100).toFixed(2)}%`;
  };

  // 파라미터 유효성 검사 함수
  const validateParams = (params: DCAStrategyParams): { [key: string]: string } => {
    const errors: { [key: string]: string } = {};

    if (!params.leverage || params.leverage <= 0 || isNaN(params.leverage)) {
      errors.leverage = '레버리지는 0보다 큰 값이어야 합니다';
    }

    if (!params.roi_net || params.roi_net <= 0 || isNaN(params.roi_net)) {
      errors.roi_net = '목표 수익률은 0보다 큰 값이어야 합니다';
    }

    if (!params.init_margin || params.init_margin <= 0 || isNaN(params.init_margin)) {
      errors.init_margin = '초기 마진은 0보다 큰 값이어야 합니다';
    }

    if (!params.add_margin || params.add_margin <= 0 || isNaN(params.add_margin)) {
      errors.add_margin = '추가 마진은 0보다 큰 값이어야 합니다';
    }

    if (!params.mmr || params.mmr <= 0 || isNaN(params.mmr)) {
      errors.mmr = '유지 증거금률은 0보다 큰 값이어야 합니다';
    }

    if (isNaN(params.fee_maker) || params.fee_maker < 0) {
      errors.fee_maker = '수수료는 0 이상의 숫자여야 합니다';
    }

    // 물타기 단계별 하락률 검사
    params.drops.forEach((drop, index) => {
      if (!drop || drop <= 0 || isNaN(drop)) {
        errors[`drop_${index}`] = `${index + 1}단계 하락률은 0보다 큰 값이어야 합니다`;
      }
    });

    return errors;
  };

  // 파라미터 업데이트 함수
  const updateParam = (key: keyof DCAStrategyParams, value: number | number[]) => {
    const newParams = { ...params };

    if (key === 'drops') {
      newParams.drops = value as number[];
    } else {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (newParams as any)[key] = value as number;
    }

    setParams(newParams);

    // 유효성 검사 실행
    const errors = validateParams(newParams);
    setValidationErrors(errors);
  };

  // 모든 파라미터가 유효한지 확인
  const isParamsValid = Object.keys(validationErrors).length === 0;
  const isFormValid = selectedFile && isParamsValid;

  return (
    <div className="min-h-screen bg-gray-50">
      <LoadingModal isVisible={loading} progress={progress} />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">
            순환매매 전략 시뮬레이션
          </h1>
          <p className="text-lg text-gray-600">
            물타기 방식의 단방향 포지션 전략
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* 설정 패널 */}
          <div className="lg:col-span-1">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <ArrowTrendingUpIcon className="h-6 w-6 text-green-600 mr-2" />
                <h2 className="text-xl font-semibold text-gray-900">시뮬레이션 설정</h2>
              </div>

              <DataFileSelector
                selectedFile={selectedFile}
                onFileChange={setSelectedFile}
              />

              {/* 전략 파라미터 */}
              <div className="space-y-4">
                <h3 className="text-lg font-medium text-gray-900">전략 파라미터</h3>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    레버리지
                  </label>
                  <input
                    type="number"
                    step="0.1"
                    value={isNaN(params.leverage) ? '' : params.leverage}
                    onChange={(e) => {
                      const value = e.target.value === '' ? NaN : parseFloat(e.target.value);
                      updateParam('leverage', value);
                    }}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.leverage
                      ? 'border-red-300 focus:ring-red-500'
                      : 'border-gray-300 focus:ring-green-500'
                      }`}
                  />
                  {validationErrors.leverage && (
                    <p className="text-sm text-red-600 mt-1">{validationErrors.leverage}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    목표 수익률 (%)
                  </label>
                  <input
                    type="number"
                    step="0.01"
                    value={isNaN(params.roi_net) ? '' : params.roi_net * 100}
                    onChange={(e) => {
                      const value = e.target.value === '' ? NaN : parseFloat(e.target.value) / 100;
                      updateParam('roi_net', value);
                    }}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.roi_net
                      ? 'border-red-300 focus:ring-red-500'
                      : 'border-gray-300 focus:ring-green-500'
                      }`}
                  />
                  {validationErrors.roi_net && (
                    <p className="text-sm text-red-600 mt-1">{validationErrors.roi_net}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    초기 마진 (USD)
                  </label>
                  <input
                    type="number"
                    step="100"
                    value={isNaN(params.init_margin) ? '' : params.init_margin}
                    onChange={(e) => {
                      const value = e.target.value === '' ? NaN : parseFloat(e.target.value);
                      updateParam('init_margin', value);
                    }}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.init_margin
                      ? 'border-red-300 focus:ring-red-500'
                      : 'border-gray-300 focus:ring-green-500'
                      }`}
                  />
                  {validationErrors.init_margin && (
                    <p className="text-sm text-red-600 mt-1">{validationErrors.init_margin}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    추가 마진 (USD)
                  </label>
                  <input
                    type="number"
                    step="100"
                    value={isNaN(params.add_margin) ? '' : params.add_margin}
                    onChange={(e) => {
                      const value = e.target.value === '' ? NaN : parseFloat(e.target.value);
                      updateParam('add_margin', value);
                    }}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.add_margin
                      ? 'border-red-300 focus:ring-red-500'
                      : 'border-gray-300 focus:ring-green-500'
                      }`}
                  />
                  {validationErrors.add_margin && (
                    <p className="text-sm text-red-600 mt-1">{validationErrors.add_margin}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    유지 증거금률 (%)
                  </label>
                  <input
                    type="number"
                    step="0.001"
                    value={isNaN(params.mmr) ? '' : params.mmr * 100}
                    onChange={(e) => {
                      const value = e.target.value === '' ? NaN : parseFloat(e.target.value) / 100;
                      updateParam('mmr', value);
                    }}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.mmr
                      ? 'border-red-300 focus:ring-red-500'
                      : 'border-gray-300 focus:ring-green-500'
                      }`}
                  />
                  {validationErrors.mmr && (
                    <p className="text-sm text-red-600 mt-1">{validationErrors.mmr}</p>
                  )}
                </div>

                {/* 물타기 단계 설정 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    물타기 단계별 하락률 (%)
                  </label>
                  <div className="space-y-2">
                    {params.drops.map((drop, index) => (
                      <div key={index} className="flex items-center">
                        <span className="text-sm text-gray-500 w-8">{index + 1}단계:</span>
                        <input
                          type="number"
                          step="0.01"
                          value={isNaN(drop) ? '' : drop * 100}
                          onChange={(e) => {
                            const newDrops = [...params.drops];
                            newDrops[index] = e.target.value === '' ? NaN : parseFloat(e.target.value) / 100;
                            updateParam('drops', newDrops);
                          }}
                          className={`flex-1 px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors[`drop_${index}`]
                            ? 'border-red-300 focus:ring-red-500'
                            : 'border-gray-300 focus:ring-green-500'
                            }`}
                        />
                        <span className="text-sm text-gray-500 ml-2">%</span>
                      </div>
                    ))}
                  </div>
                  {Object.keys(validationErrors).some(key => key.startsWith('drop_')) && (
                    <div className="mt-2">
                      {Object.keys(validationErrors)
                        .filter(key => key.startsWith('drop_'))
                        .map(key => (
                          <p key={key} className="text-sm text-red-600">{validationErrors[key]}</p>
                        ))}
                    </div>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    수수료 (Maker)
                  </label>
                  <input
                    type="number"
                    step="0.0001"
                    value={isNaN(params.fee_maker) ? '' : params.fee_maker * 100}
                    onChange={(e) => {
                      const value = e.target.value === '' ? NaN : parseFloat(e.target.value) / 100;
                      updateParam('fee_maker', value);
                    }}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.fee_maker
                      ? 'border-red-300 focus:ring-red-500'
                      : 'border-gray-300 focus:ring-green-500'
                      }`}
                  />
                  {validationErrors.fee_maker && (
                    <p className="text-sm text-red-600 mt-1">{validationErrors.fee_maker}</p>
                  )}
                </div>
              </div>

              {/* 실행 버튼 */}
              <button
                onClick={runSimulation}
                disabled={loading || !isFormValid}
                className="w-full mt-6 bg-green-600 text-white py-3 px-4 rounded-md hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center"
              >
                {loading ? (
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                ) : (
                  <>
                    <PlayIcon className="h-5 w-5 mr-2" />
                    시뮬레이션 실행
                  </>
                )}
              </button>

              {error && (
                <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-md">
                  <div className="flex">
                    <ExclamationTriangleIcon className="h-5 w-5 text-red-400 mr-2" />
                    <p className="text-sm text-red-700">{error}</p>
                  </div>
                </div>
              )}

              {/* 유효성 검사 상태 표시 */}
              {!isFormValid && (
                <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-md">
                  <div className="flex items-center">
                    <ExclamationTriangleIcon className="h-4 w-4 text-yellow-400 mr-2" />
                    <div>
                      <p className="text-sm text-yellow-700 font-medium">
                        모든 파라미터를 올바르게 입력해주세요
                      </p>
                      {!selectedFile && (
                        <p className="text-xs text-yellow-600 mt-1">• 데이터 파일을 선택해주세요</p>
                      )}
                      {!isParamsValid && (
                        <p className="text-xs text-yellow-600 mt-1">• 빨간색으로 표시된 파라미터를 수정해주세요</p>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {/* 사용 안내 정보 */}
              <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-md">
                <div className="flex items-center">
                  <PlayIcon className="h-4 w-4 text-green-400 mr-2" />
                  <p className="text-sm text-green-700">
                    파라미터를 조정한 후 &quot;시뮬레이션 실행&quot; 버튼을 클릭하세요
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* 결과 패널 */}
          <div className="lg:col-span-2">
            {result ? (
              <div className="space-y-6">
                {/* 요약 통계 */}
                <div className="bg-white rounded-lg shadow-md p-6">
                  <div className="flex items-center mb-4">
                    <ChartBarIcon className="h-6 w-6 text-green-600 mr-2" />
                    <h2 className="text-xl font-semibold text-gray-900">시뮬레이션 결과</h2>
                  </div>

                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="text-center p-4 bg-green-50 rounded-lg">
                      <div className="text-2xl font-bold text-green-600">
                        {formatCurrency(result.summary.total_realized_pnl_usd)}
                      </div>
                      <div className="text-sm text-green-700">총 수익</div>
                    </div>

                    <div className="text-center p-4 bg-blue-50 rounded-lg">
                      <div className="text-2xl font-bold text-blue-600">
                        {formatPercentage(result.summary.win_rate)}
                      </div>
                      <div className="text-sm text-blue-700">TP 달성률</div>
                    </div>

                    <div className="text-center p-4 bg-purple-50 rounded-lg">
                      <div className="text-2xl font-bold text-purple-600">
                        {result.summary.wins_positive_events + result.summary.loss_negative_events}
                      </div>
                      <div className="text-sm text-purple-700">총 거래</div>
                    </div>

                    <div className="text-center p-4 bg-orange-50 rounded-lg">
                      <div className="text-2xl font-bold text-orange-600">
                        {result.summary.wins_positive_events}
                      </div>
                      <div className="text-sm text-orange-700">TP 달성</div>
                    </div>
                  </div>

                  <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-600">LIQ 발생:</span>
                      <span className="font-medium">{result.summary.loss_negative_events}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">OPEN 상태:</span>
                      <span className="font-medium">{result.summary.preclose_resets}</span>
                    </div>
                  </div>
                </div>

                {/* 에쿼티 곡선 */}
                <EquityCurve data={result.equity_data} />

                {/* 이벤트 로그 */}
                {result.events_data.length > 0 && (
                  <div className="bg-white rounded-lg shadow-md p-6">
                    <div className="flex items-center mb-4">
                      <DocumentTextIcon className="h-6 w-6 text-gray-600 mr-2" />
                      <h2 className="text-xl font-semibold text-gray-900">거래 이벤트</h2>
                    </div>

                    <div className="overflow-x-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              진입 시간
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              결과
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              물타기 단계
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              수익 (USD)
                            </th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              보유 시간
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {result.events_data.slice(0, 10).map((event, index) => (
                            <tr key={index}>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                {event.Entry_Time && typeof event.Entry_Time === 'string' ? (() => {
                                  const date = new Date(event.Entry_Time);
                                  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
                                })() : '-'}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                {event.Result && typeof event.Result === 'string' ? event.Result : '-'}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                {event.WaterCnt !== undefined ? `${event.WaterCnt}단계` : '-'}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm">
                                {event.Profit && typeof event.Profit === 'number' ? (
                                  <span className={event.Profit >= 0 ? 'text-green-600' : 'text-red-600'}>
                                    {formatCurrency(event.Profit)}
                                  </span>
                                ) : '-'}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                {event.Hold_Min !== undefined ? `${event.Hold_Min}분` : '-'}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    {result.events_data.length > 10 && (
                      <p className="text-sm text-gray-500 mt-2">
                        최근 10개 이벤트만 표시됩니다. 총 {result.events_data.length}개 이벤트가 있습니다.
                      </p>
                    )}
                  </div>
                )}
              </div>
            ) : (
              <div className="bg-white rounded-lg shadow-md p-6">
                <div className="text-center py-12">
                  <CurrencyDollarIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">
                    시뮬레이션을 실행하세요
                  </h3>
                  <p className="text-gray-500">
                    왼쪽 패널에서 설정을 조정하고 시뮬레이션을 실행하면 결과를 확인할 수 있습니다.
                  </p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
