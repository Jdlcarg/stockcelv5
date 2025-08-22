import { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiRequest } from '@/lib/queryClient';
import { Play, Square, Activity, AlertTriangle, CheckCircle, Clock } from 'lucide-react';
import Header from '@/components/layout/header';
import Sidebar from '@/components/layout/sidebar';

interface AutoSyncStatus {
  isRunning: boolean;
  lastCheck: string | null;
  nextCheck: string | null;
  issuesFound: number;
  issuesFixed: number;
  intervalSeconds: number;
  startedAt: string | null;
}

export default function AutoSyncMonitor() {
  const queryClient = useQueryClient();
  const [logs, setLogs] = useState<string[]>([]);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  // Get auto-sync status
  const { data: status, isLoading } = useQuery<AutoSyncStatus>({
    queryKey: ['/api/auto-sync/status'],
    refetchInterval: 2000, // Refresh every 2 seconds
  });

  // Start monitor mutation
  const startMutation = useMutation({
    mutationFn: () => apiRequest('/api/auto-sync/start', 'POST'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/auto-sync/status'] });
      setLogs(prev => [...prev, `✅ [${new Date().toLocaleTimeString()}] Monitor iniciado correctamente`]);
    },
    onError: (error: any) => {
      setLogs(prev => [...prev, `❌ [${new Date().toLocaleTimeString()}] Error al iniciar: ${error.message}`]);
    }
  });

  // Stop monitor mutation
  const stopMutation = useMutation({
    mutationFn: () => apiRequest('/api/auto-sync/stop', 'POST'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/auto-sync/status'] });
      setLogs(prev => [...prev, `🛑 [${new Date().toLocaleTimeString()}] Monitor detenido correctamente`]);
    },
    onError: (error: any) => {
      setLogs(prev => [...prev, `❌ [${new Date().toLocaleTimeString()}] Error al detener: ${error.message}`]);
    }
  });

  // Add status change logs
  useEffect(() => {
    if (status) {
      const timestamp = new Date().toLocaleTimeString();
      if (status.isRunning && status.lastCheck) {
        setLogs(prev => {
          const lastLog = prev[prev.length - 1];
          const newLogMessage = `🔍 [${timestamp}] Verificación completada - ${status.issuesFound} problemas encontrados, ${status.issuesFixed} corregidos`;
          
          // Avoid duplicate logs
          if (lastLog !== newLogMessage) {
            return [...prev.slice(-19), newLogMessage]; // Keep last 20 logs
          }
          return prev;
        });
      }
    }
  }, [status?.lastCheck, status?.issuesFound, status?.issuesFixed]);

  if (isLoading) {
    return (
      <div className="flex h-screen bg-background">
        <Sidebar />
        <div className="flex-1 flex flex-col">
          <Header onMobileMenuToggle={() => setIsMobileMenuOpen(!isMobileMenuOpen)} />
          <main className="flex-1 p-6">
            <div className="flex items-center justify-center h-64">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
            </div>
          </main>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <div className="flex-1 flex flex-col">
        <Header onMobileMenuToggle={() => setIsMobileMenuOpen(!isMobileMenuOpen)} />
        <main className="flex-1 p-6 space-y-6">
          <div className="mb-6">
            <h1 className="text-3xl font-bold tracking-tight">Monitor Automático de Sincronización</h1>
            <p className="text-muted-foreground">Sistema de monitoreo y corrección automática de sincronización</p>
          </div>
          
          {/* Status Overview */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Estado</CardTitle>
                <Activity className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="flex items-center space-x-2">
                  <Badge variant={status?.isRunning ? "default" : "secondary"}>
                    {status?.isRunning ? "Activo" : "Inactivo"}
                  </Badge>
                  {status?.isRunning && (
                    <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                  )}
                </div>
                <p className="text-xs text-muted-foreground mt-1">
                  Verificación cada {status?.intervalSeconds || 5} segundos
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Problemas Detectados</CardTitle>
                <AlertTriangle className="h-4 w-4 text-orange-500" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-orange-600">
                  {status?.issuesFound || 0}
                </div>
                <p className="text-xs text-muted-foreground">
                  Total encontrados
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Problemas Corregidos</CardTitle>
                <CheckCircle className="h-4 w-4 text-green-500" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-green-600">
                  {status?.issuesFixed || 0}
                </div>
                <p className="text-xs text-muted-foreground">
                  Auto-corregidos
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Próxima Verificación</CardTitle>
                <Clock className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-sm font-bold">
                  {status?.nextCheck ? 
                    new Date(status.nextCheck).toLocaleTimeString() : 
                    "No programada"
                  }
                </div>
                <p className="text-xs text-muted-foreground">
                  Siguiente ejecución
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Control Panel */}
          <Card>
            <CardHeader>
              <CardTitle>Panel de Control</CardTitle>
              <CardDescription>
                Controla el sistema automático de monitoreo y corrección de sincronización
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex space-x-2">
                <Button
                  onClick={() => startMutation.mutate()}
                  disabled={status?.isRunning || startMutation.isPending}
                  className="flex items-center space-x-2"
                >
                  <Play className="h-4 w-4" />
                  <span>Iniciar Monitor</span>
                </Button>
                
                <Button
                  onClick={() => stopMutation.mutate()}
                  disabled={!status?.isRunning || stopMutation.isPending}
                  variant="destructive"
                  className="flex items-center space-x-2"
                >
                  <Square className="h-4 w-4" />
                  <span>Detener Monitor</span>
                </Button>
              </div>

              {status?.isRunning && (
                <Alert>
                  <Activity className="h-4 w-4" />
                  <AlertDescription>
                    El monitor está ejecutándose automáticamente cada {status.intervalSeconds} segundos. 
                    Detectará y corregirá problemas de sincronización entre pagos y movimientos de caja en tiempo real.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>

          {/* Detailed Information */}
          <Card>
            <CardHeader>
              <CardTitle>Información Detallada</CardTitle>
            </CardHeader>
            <CardContent>
              <Tabs defaultValue="status" className="w-full">
                <TabsList>
                  <TabsTrigger value="status">Estado Actual</TabsTrigger>
                  <TabsTrigger value="logs">Logs del Sistema</TabsTrigger>
                  <TabsTrigger value="config">Configuración</TabsTrigger>
                </TabsList>
                
                <TabsContent value="status" className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <h4 className="font-semibold mb-2">Información del Monitor</h4>
                      <div className="space-y-1 text-sm">
                        <p><strong>Estado:</strong> {status?.isRunning ? "Ejecutándose" : "Detenido"}</p>
                        <p><strong>Iniciado en:</strong> {
                          status?.startedAt ? 
                            new Date(status.startedAt).toLocaleString() : 
                            "No iniciado"
                        }</p>
                        <p><strong>Última verificación:</strong> {
                          status?.lastCheck ? 
                            new Date(status.lastCheck).toLocaleString() : 
                            "Nunca"
                        }</p>
                      </div>
                    </div>
                    
                    <div>
                      <h4 className="font-semibold mb-2">Estadísticas</h4>
                      <div className="space-y-1 text-sm">
                        <p><strong>Intervalo:</strong> {status?.intervalSeconds} segundos</p>
                        <p><strong>Problemas encontrados:</strong> {status?.issuesFound}</p>
                        <p><strong>Problemas corregidos:</strong> {status?.issuesFixed}</p>
                        <p><strong>Efectividad:</strong> {
                          status?.issuesFound ? 
                            Math.round((status.issuesFixed / status.issuesFound) * 100) + "%" :
                            "N/A"
                        }</p>
                      </div>
                    </div>
                  </div>
                </TabsContent>
                
                <TabsContent value="logs" className="space-y-4">
                  <div>
                    <h4 className="font-semibold mb-2">Logs en Tiempo Real</h4>
                    <div className="bg-black text-green-400 p-4 rounded-lg font-mono text-sm max-h-64 overflow-y-auto">
                      {logs.length === 0 ? (
                        <p className="text-gray-500">No hay logs disponibles...</p>
                      ) : (
                        logs.map((log, index) => (
                          <div key={index} className="mb-1">
                            {log}
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </TabsContent>
                
                <TabsContent value="config" className="space-y-4">
                  <div>
                    <h4 className="font-semibold mb-2">Configuración del Sistema</h4>
                    <div className="space-y-2 text-sm">
                      <p><strong>Función:</strong> Detecta y corrige automáticamente desincronizaciones entre pagos y movimientos de caja</p>
                      <p><strong>Alcance:</strong> Verifica todas las órdenes del día actual con pagos registrados</p>
                      <p><strong>Acción:</strong> Crea automáticamente movimientos de caja faltantes para mantener sincronización perfecta</p>
                      <p><strong>Frecuencia:</strong> Cada {status?.intervalSeconds || 5} segundos durante horario operativo</p>
                      <p><strong>Logging:</strong> Registra todas las acciones con prefijos [AUTO-SYNC] para debugging</p>
                    </div>
                  </div>
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>

        </main>
      </div>
    </div>
  );
}