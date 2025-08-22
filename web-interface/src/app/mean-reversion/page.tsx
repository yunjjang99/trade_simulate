'use client';

import { useState, useCallback } from 'react';
import {
    CogIcon,
    PlayIcon,
    ExclamationTriangleIcon,
    ChartBarIcon,
    DocumentTextIcon,
    CurrencyDollarIcon
} from '@heroicons/react/24/outline';
import { SimulationResult, parseCSVData, MeanReversionParams } from '@/lib/simulation';
import DataFileSelector from '@/components/DataFileSelector';
import LoadingModal from '@/components/LoadingModal';
import EquityCurve from '@/components/EquityCurve';

const SCENARIOS = [
    { id: 'weekend', name: 'Weekend (토/일)', description: '뉴욕시간 토요일, 일요일만 거래' },
    { id: 'us_closed', name: 'US Closed', description: '미국 주식장 외 시간' },
    { id: 'all', name: 'All Time', description: '전체 시간 (ATR 게이팅 적용)' }
];

export default function MeanReversionPage() {
    const [selectedFile, setSelectedFile] = useState<string>('');
    const [selectedScenario, setSelectedScenario] = useState<string>('weekend');
    const [params, setParams] = useState<MeanReversionParams>({
        leverage: 12.0,
        leg_margin: 1000.0,
        price_move_pct: 0.0035,
        partial_reduce: 0.30,
        atr_factor: 0.4,
        preclose_minutes: 45,
        immediate_reopen: true,
        fee_maker: 0.0002
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
                strategyType: 'mean_reversion',
                scenario: selectedScenario,
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
    const validateParams = (params: MeanReversionParams): { [key: string]: string } => {
        const errors: { [key: string]: string } = {};

        if (!params.leverage || params.leverage <= 0 || isNaN(params.leverage)) {
            errors.leverage = '레버리지는 0보다 큰 값이어야 합니다';
        }

        if (!params.leg_margin || params.leg_margin <= 0 || isNaN(params.leg_margin)) {
            errors.leg_margin = '레그 마진은 0보다 큰 값이어야 합니다';
        }

        if (!params.price_move_pct || params.price_move_pct <= 0 || isNaN(params.price_move_pct)) {
            errors.price_move_pct = '가격 변동 기준은 0보다 큰 값이어야 합니다';
        }

        if (!params.partial_reduce || params.partial_reduce <= 0 || isNaN(params.partial_reduce)) {
            errors.partial_reduce = '부분 축소 비율은 0보다 큰 값이어야 합니다';
        }

        if (!params.atr_factor || params.atr_factor <= 0 || isNaN(params.atr_factor)) {
            errors.atr_factor = 'ATR 팩터는 0보다 큰 값이어야 합니다';
        }

        if (!params.preclose_minutes || params.preclose_minutes <= 0 || isNaN(params.preclose_minutes)) {
            errors.preclose_minutes = '프리클로즈 시간은 0보다 큰 값이어야 합니다';
        }

        if (isNaN(params.fee_maker) || params.fee_maker < 0) {
            errors.fee_maker = '수수료는 0 이상의 숫자여야 합니다';
        }

        return errors;
    };

    // 파라미터 업데이트 함수
    const updateParam = (key: keyof MeanReversionParams, value: number | boolean) => {
        const newParams = { ...params };
        (newParams as Record<string, number | boolean>)[key] = value;
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
                        횡보장 전략 시뮬레이션
                    </h1>
                    <p className="text-lg text-gray-600">
                        양방향 포지션으로 평균회귀를 노리는 전략
                    </p>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                    {/* 설정 패널 */}
                    <div className="lg:col-span-1">
                        <div className="bg-white rounded-lg shadow-md p-6">
                            <div className="flex items-center mb-4">
                                <CogIcon className="h-6 w-6 text-blue-600 mr-2" />
                                <h2 className="text-xl font-semibold text-gray-900">시뮬레이션 설정</h2>
                            </div>

                            <DataFileSelector
                                selectedFile={selectedFile}
                                onFileChange={setSelectedFile}
                            />

                            {/* 시나리오 선택 */}
                            <div className="mb-6">
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    거래 시나리오
                                </label>
                                <select
                                    value={selectedScenario}
                                    onChange={(e) => setSelectedScenario(e.target.value)}
                                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                >
                                    {SCENARIOS.map((scenario) => (
                                        <option key={scenario.id} value={scenario.id}>
                                            {scenario.name}
                                        </option>
                                    ))}
                                </select>
                                {SCENARIOS.find(s => s.id === selectedScenario) && (
                                    <p className="text-sm text-gray-500 mt-1">
                                        {SCENARIOS.find(s => s.id === selectedScenario)?.description}
                                    </p>
                                )}
                            </div>

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
                                            : 'border-gray-300 focus:ring-blue-500'
                                            }`}
                                    />
                                    {validationErrors.leverage && (
                                        <p className="text-sm text-red-600 mt-1">{validationErrors.leverage}</p>
                                    )}
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        레그 마진 (USD)
                                    </label>
                                    <input
                                        type="number"
                                        step="100"
                                        value={isNaN(params.leg_margin) ? '' : params.leg_margin}
                                        onChange={(e) => {
                                            const value = e.target.value === '' ? NaN : parseFloat(e.target.value);
                                            updateParam('leg_margin', value);
                                        }}
                                        className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.leg_margin
                                            ? 'border-red-300 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-blue-500'
                                            }`}
                                    />
                                    {validationErrors.leg_margin && (
                                        <p className="text-sm text-red-600 mt-1">{validationErrors.leg_margin}</p>
                                    )}
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        가격 변동 기준 (%)
                                    </label>
                                    <input
                                        type="number"
                                        step="0.0001"
                                        value={isNaN(params.price_move_pct) ? '' : params.price_move_pct * 100}
                                        onChange={(e) => {
                                            const value = e.target.value === '' ? NaN : parseFloat(e.target.value) / 100;
                                            updateParam('price_move_pct', value);
                                        }}
                                        className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.price_move_pct
                                            ? 'border-red-300 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-blue-500'
                                            }`}
                                    />
                                    {validationErrors.price_move_pct && (
                                        <p className="text-sm text-red-600 mt-1">{validationErrors.price_move_pct}</p>
                                    )}
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        부분 축소 비율 (%)
                                    </label>
                                    <input
                                        type="number"
                                        step="0.01"
                                        value={isNaN(params.partial_reduce) ? '' : params.partial_reduce * 100}
                                        onChange={(e) => {
                                            const value = e.target.value === '' ? NaN : parseFloat(e.target.value) / 100;
                                            updateParam('partial_reduce', value);
                                        }}
                                        className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.partial_reduce
                                            ? 'border-red-300 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-blue-500'
                                            }`}
                                    />
                                    {validationErrors.partial_reduce && (
                                        <p className="text-sm text-red-600 mt-1">{validationErrors.partial_reduce}</p>
                                    )}
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        ATR 팩터
                                    </label>
                                    <input
                                        type="number"
                                        step="0.1"
                                        value={isNaN(params.atr_factor) ? '' : params.atr_factor}
                                        onChange={(e) => {
                                            const value = e.target.value === '' ? NaN : parseFloat(e.target.value);
                                            updateParam('atr_factor', value);
                                        }}
                                        className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.atr_factor
                                            ? 'border-red-300 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-blue-500'
                                            }`}
                                    />
                                    {validationErrors.atr_factor && (
                                        <p className="text-sm text-red-600 mt-1">{validationErrors.atr_factor}</p>
                                    )}
                                </div>

                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        프리클로즈 시간 (분)
                                    </label>
                                    <input
                                        type="number"
                                        step="5"
                                        value={isNaN(params.preclose_minutes) ? '' : params.preclose_minutes}
                                        onChange={(e) => {
                                            const value = e.target.value === '' ? NaN : parseInt(e.target.value);
                                            updateParam('preclose_minutes', value);
                                        }}
                                        className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 ${validationErrors.preclose_minutes
                                            ? 'border-red-300 focus:ring-red-500'
                                            : 'border-gray-300 focus:ring-blue-500'
                                            }`}
                                    />
                                    {validationErrors.preclose_minutes && (
                                        <p className="text-sm text-red-600 mt-1">{validationErrors.preclose_minutes}</p>
                                    )}
                                </div>

                                <div className="flex items-center">
                                    <input
                                        type="checkbox"
                                        id="immediate_reopen"
                                        checked={params.immediate_reopen}
                                        onChange={(e) => updateParam('immediate_reopen', e.target.checked)}
                                        className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                                    />
                                    <label htmlFor="immediate_reopen" className="ml-2 block text-sm text-gray-700">
                                        즉시 재진입 허용
                                    </label>
                                </div>

                                {/* 수수료 설정 */}
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-1">
                                        수수료 (Maker) (%)
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
                                            : 'border-gray-300 focus:ring-blue-500'
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
                                className="w-full mt-6 bg-blue-600 text-white py-3 px-4 rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center"
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
                            <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
                                <div className="flex items-center">
                                    <PlayIcon className="h-4 w-4 text-blue-400 mr-2" />
                                    <p className="text-sm text-blue-700">
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
                                            <div className="text-sm text-blue-700">승률</div>
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
                                            <div className="text-sm text-orange-700">수익 거래</div>
                                        </div>
                                    </div>

                                    <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
                                        <div className="flex justify-between">
                                            <span className="text-gray-600">손실 거래:</span>
                                            <span className="font-medium">{result.summary.loss_negative_events}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-gray-600">프리클로즈 리셋:</span>
                                            <span className="font-medium">{result.summary.preclose_resets}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-gray-600">강제 청산:</span>
                                            <span className="font-medium">{result.summary.forced_closes}</span>
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
                                                            시간
                                                        </th>
                                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                            이벤트
                                                        </th>
                                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                            PnL 변화
                                                        </th>
                                                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                                            누적 PnL
                                                        </th>
                                                    </tr>
                                                </thead>
                                                <tbody className="bg-white divide-y divide-gray-200">
                                                    {result.events_data.slice(0, 10).map((event, index) => (
                                                        <tr key={index}>
                                                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                                                {event.time && typeof event.time === 'string' ? (() => {
                                                                    const date = new Date(event.time);
                                                                    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
                                                                })() : '-'}
                                                            </td>
                                                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                                                {event.event && typeof event.event === 'string' ? event.event : '-'}
                                                            </td>
                                                            <td className="px-6 py-4 whitespace-nowrap text-sm">
                                                                {event.net_pnl_change && typeof event.net_pnl_change === 'number' ? (
                                                                    <span className={event.net_pnl_change >= 0 ? 'text-green-600' : 'text-red-600'}>
                                                                        {formatCurrency(event.net_pnl_change)}
                                                                    </span>
                                                                ) : '-'}
                                                            </td>
                                                            <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                                                {event.pnl_cum && typeof event.pnl_cum === 'number' ? formatCurrency(event.pnl_cum) : '-'}
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
