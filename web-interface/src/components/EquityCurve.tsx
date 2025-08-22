'use client';

import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { ChartBarIcon } from '@heroicons/react/24/outline';

interface EquityData {
    time: string;
    cum_pnl: number;
}

interface EquityCurveProps {
    data: EquityData[];
}

const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2
    }).format(value);
};

export default function EquityCurve({ data }: EquityCurveProps) {
    if (data.length === 0) return null;

    return (
        <div className="bg-white rounded-lg shadow-md p-6">
            <div className="flex items-center mb-4">
                <ChartBarIcon className="h-6 w-6 text-blue-600 mr-2" />
                <h2 className="text-xl font-semibold text-gray-900">에쿼티 곡선</h2>
            </div>

            <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                            dataKey="time"
                            tickFormatter={(value) => {
                                const date = new Date(value);
                                return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
                            }}
                        />
                        <YAxis />
                        <Tooltip
                            labelFormatter={(value) => {
                                const date = new Date(value);
                                return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
                            }}
                            formatter={(value: number) => [formatCurrency(value), '누적 PnL']}
                        />
                        <Line
                            type="monotone"
                            dataKey="cum_pnl"
                            stroke="#3B82F6"
                            strokeWidth={2}
                            dot={false}
                        />
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}
