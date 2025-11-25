import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Play, Square, Settings, RefreshCw, CheckCircle, AlertTriangle } from 'lucide-react';
import { api } from '../services/api';

const PipelineControl = () => {
    const [isRunning, setIsRunning] = useState(false);
    const [logs, setLogs] = useState([]);
    const [config, setConfig] = useState({
        asins: 'B0CX59H5W7,B0FHB5V36G',
        pages: 2,
        headless: true,
        mining: true
    });

    // Poll for status
    useEffect(() => {
        const checkStatus = async () => {
            const status = await api.getStatus();
            if (status) {
                setIsRunning(status.status === 'running');
                if (status.message && status.message !== 'Ready') {
                    addLog(status.message, status.status === 'running' ? 'info' : 'success');
                }
            }
        };

        const interval = setInterval(checkStatus, 2000);
        return () => clearInterval(interval);
    }, []);

    const handleStart = async () => {
        try {
            addLog('Initiating pipeline start...', 'info');
            await api.startPipeline(config);
            setIsRunning(true);
            addLog('Pipeline started successfully', 'success');
        } catch (error) {
            addLog('Failed to start pipeline', 'warning');
        }
    };

    const handleStop = async () => {
        try {
            await api.stopPipeline();
            setIsRunning(false);
            addLog('Pipeline stop requested', 'warning');
        } catch (error) {
            addLog('Failed to stop pipeline', 'warning');
        }
    };

    const addLog = (message, type) => {
        setLogs(prev => [...prev, { message, type, timestamp: new Date().toLocaleTimeString() }].slice(-50));
    };

    return (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[calc(100vh-100px)]">
            {/* Configuration Panel */}
            <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                className="glass-panel p-6 lg:col-span-1 flex flex-col"
            >
                <h2 className="text-xl font-orbitron mb-6 flex items-center">
                    <Settings className="mr-2 text-neon-blue" />
                    Configuration
                </h2>

                <div className="space-y-4 flex-1">
                    <div>
                        <label className="block text-sm text-gray-400 mb-1">Target ASINs (comma separated)</label>
                        <textarea
                            value={config.asins}
                            onChange={(e) => setConfig({ ...config, asins: e.target.value })}
                            className="w-full bg-dark-bg border border-white/10 rounded-lg p-3 text-sm focus:border-neon-blue focus:outline-none h-32 font-mono"
                        />
                    </div>

                    <div>
                        <label className="block text-sm text-gray-400 mb-1">Pages per Product</label>
                        <input
                            type="number"
                            value={config.pages}
                            onChange={(e) => setConfig({ ...config, pages: parseInt(e.target.value) })}
                            className="w-full bg-dark-bg border border-white/10 rounded-lg p-3 text-sm focus:border-neon-blue focus:outline-none"
                        />
                    </div>

                    <div className="flex items-center justify-between p-3 bg-white/5 rounded-lg">
                        <span className="text-sm">Headless Mode</span>
                        <button
                            onClick={() => setConfig({ ...config, headless: !config.headless })}
                            className={`w-12 h-6 rounded-full transition-colors relative ${config.headless ? 'bg-neon-green' : 'bg-gray-600'}`}
                        >
                            <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-all ${config.headless ? 'left-7' : 'left-1'}`} />
                        </button>
                    </div>

                    <div className="flex items-center justify-between p-3 bg-white/5 rounded-lg">
                        <span className="text-sm">Review Mining</span>
                        <button
                            onClick={() => setConfig({ ...config, mining: !config.mining })}
                            className={`w-12 h-6 rounded-full transition-colors relative ${config.mining ? 'bg-neon-purple' : 'bg-gray-600'}`}
                        >
                            <div className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-all ${config.mining ? 'left-7' : 'left-1'}`} />
                        </button>
                    </div>
                </div>

                <div className="mt-6 pt-6 border-t border-white/10">
                    {!isRunning ? (
                        <button
                            onClick={handleStart}
                            className="w-full btn-primary flex items-center justify-center space-x-2 group"
                        >
                            <Play size={18} className="group-hover:fill-current" />
                            <span>Start Pipeline</span>
                        </button>
                    ) : (
                        <button
                            onClick={handleStop}
                            className="w-full px-6 py-2 bg-neon-pink/10 border border-neon-pink text-neon-pink rounded-lg hover:bg-neon-pink/20 transition-all font-orbitron tracking-wider uppercase text-sm flex items-center justify-center space-x-2"
                        >
                            <Square size={18} fill="currentColor" />
                            <span>Stop Pipeline</span>
                        </button>
                    )}
                </div>
            </motion.div>

            {/* Live Logs Panel */}
            <motion.div
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                className="glass-panel p-6 lg:col-span-2 flex flex-col"
            >
                <div className="flex justify-between items-center mb-6">
                    <h2 className="text-xl font-orbitron flex items-center">
                        <RefreshCw className={`mr-2 text-neon-green ${isRunning ? 'animate-spin' : ''}`} />
                        Live Execution Logs
                    </h2>
                    <span className="text-xs font-mono text-gray-500">ID: {Math.random().toString(36).substr(2, 9)}</span>
                </div>

                <div className="flex-1 bg-dark-bg rounded-lg p-4 overflow-y-auto font-mono text-sm space-y-2 border border-white/5">
                    {logs.length === 0 && (
                        <div className="h-full flex items-center justify-center text-gray-600 italic">
                            Ready to start pipeline...
                        </div>
                    )}
                    {logs.map((log, index) => (
                        <motion.div
                            key={index}
                            initial={{ opacity: 0, x: -10 }}
                            animate={{ opacity: 1, x: 0 }}
                            className="flex items-start space-x-3"
                        >
                            <span className="text-gray-600 shrink-0">[{log.timestamp}]</span>
                            <span className={`
                ${log.type === 'info' ? 'text-blue-400' : ''}
                ${log.type === 'success' ? 'text-neon-green' : ''}
                ${log.type === 'warning' ? 'text-neon-pink' : ''}
              `}>
                                {log.type === 'success' && <CheckCircle size={14} className="inline mr-1" />}
                                {log.type === 'warning' && <AlertTriangle size={14} className="inline mr-1" />}
                                {log.message}
                            </span>
                        </motion.div>
                    ))}
                    {isRunning && (
                        <div className="flex items-center space-x-2 text-gray-500 animate-pulse">
                            <span>_</span>
                        </div>
                    )}
                </div>
            </motion.div>
        </div>
    );
};

export default PipelineControl;
