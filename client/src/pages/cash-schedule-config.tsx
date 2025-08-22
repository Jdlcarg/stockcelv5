
import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog";
import { 
  Clock, 
  Calendar, 
  Settings, 
  Plus, 
  Trash2, 
  Edit, 
  Save, 
  RotateCcw, 
  Activity, 
  Clock3, 
  Users,
  CheckCircle,
  XCircle,
  AlertCircle,
  RefreshCw,
  Play,
  Pause,
  Eye,
  Zap
} from "lucide-react";

interface SchedulePeriod {
  id?: number;
  clientId: number;
  name: string;
  startHour: number;
  startMinute: number;
  endHour: number;
  endMinute: number;
  daysOfWeek: string; // "1,2,3,4,5" formato
  autoOpenEnabled: boolean;
  autoCloseEnabled: boolean;
  isActive: boolean;
  timezone: string;
  createdAt?: string;
  updatedAt?: string;
}

interface OperationLog {
  id: number;
  clientId: number;
  type: 'auto_open' | 'auto_close';
  scheduledTime: string;
  executedTime: string;
  status: 'success' | 'failed' | 'pending';
  periodName: string;
  errorMessage?: string;
}

const DAYS_OF_WEEK = [
  { value: "1", label: "Lunes" },
  { value: "2", label: "Martes" },
  { value: "3", label: "Miércoles" },
  { value: "4", label: "Jueves" },
  { value: "5", label: "Viernes" },
  { value: "6", label: "Sábado" },
  { value: "7", label: "Domingo" },
];

export default function CashScheduleConfig() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Dialog states
  const [periodDialogOpen, setPeriodDialogOpen] = useState(false);
  const [editingPeriod, setEditingPeriod] = useState<SchedulePeriod | null>(null);

  // Form state for new/edit period
  const [periodForm, setPeriodForm] = useState<Partial<SchedulePeriod>>({
    name: "",
    startHour: 9,
    startMinute: 0,
    endHour: 18,
    endMinute: 0,
    daysOfWeek: "1,2,3,4,5",
    autoOpenEnabled: true,
    autoCloseEnabled: true,
    isActive: true,
    timezone: "America/Argentina/Buenos_Aires"
  });

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Get schedule periods
  const { data: periods = [], isLoading: periodsLoading, refetch: refetchPeriods } = useQuery({
    queryKey: ["/api/cash-schedule/periods", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-schedule/periods?clientId=${user?.clientId}`);
      if (!response.ok) throw new Error('Failed to fetch periods');
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Get automation service status
  const { data: serviceStatus } = useQuery({
    queryKey: ["/api/cash-schedule/service-status"],
    queryFn: async () => {
      const response = await fetch("/api/cash-schedule/service-status");
      if (!response.ok) throw new Error('Failed to fetch service status');
      return response.json();
    },
    refetchInterval: 10000,
  });

  // Get operations log
  const { data: operationsLog = [] } = useQuery({
    queryKey: ["/api/cash-schedule/operations-log", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-schedule/operations-log?clientId=${user?.clientId}&limit=50`);
      if (!response.ok) throw new Error('Failed to fetch operations log');
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: 30000,
  });

  // Get next scheduled operations
  const { data: nextOperations = [] } = useQuery({
    queryKey: ["/api/cash-schedule/next-operations", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-schedule/next-operations?clientId=${user?.clientId}&limit=10`);
      if (!response.ok) throw new Error('Failed to fetch next operations');
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: 60000,
  });

  // Save period mutation
  const savePeriodMutation = useMutation({
    mutationFn: async (periodData: Partial<SchedulePeriod>) => {
      const url = editingPeriod 
        ? `/api/cash-schedule/periods/${editingPeriod.id}`
        : '/api/cash-schedule/periods';
      const method = editingPeriod ? 'PUT' : 'POST';
      
      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...periodData,
          clientId: user?.clientId
        }),
      });
      
      if (!response.ok) throw new Error('Failed to save period');
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "✅ Período guardado",
        description: editingPeriod ? "Período actualizado correctamente" : "Nuevo período creado correctamente",
      });
      setPeriodDialogOpen(false);
      setEditingPeriod(null);
      resetPeriodForm();
      refetchPeriods();
      queryClient.invalidateQueries({ queryKey: ["/api/cash-schedule/next-operations"] });
    },
    onError: () => {
      toast({
        title: "❌ Error",
        description: "Error al guardar el período",
        variant: "destructive",
      });
    },
  });

  // Delete period mutation
  const deletePeriodMutation = useMutation({
    mutationFn: async (periodId: number) => {
      const response = await fetch(`/api/cash-schedule/periods/${periodId}`, {
        method: 'DELETE',
      });
      if (!response.ok) throw new Error('Failed to delete period');
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "✅ Período eliminado",
        description: "El período ha sido eliminado correctamente",
      });
      refetchPeriods();
      queryClient.invalidateQueries({ queryKey: ["/api/cash-schedule/next-operations"] });
    },
    onError: () => {
      toast({
        title: "❌ Error",
        description: "Error al eliminar el período",
        variant: "destructive",
      });
    },
  });

  const resetPeriodForm = () => {
    setPeriodForm({
      name: "",
      startHour: 9,
      startMinute: 0,
      endHour: 18,
      endMinute: 0,
      daysOfWeek: "1,2,3,4,5",
      autoOpenEnabled: true,
      autoCloseEnabled: true,
      isActive: true,
      timezone: "America/Argentina/Buenos_Aires"
    });
  };

  const openEditDialog = (period: SchedulePeriod) => {
    setEditingPeriod(period);
    setPeriodForm({
      name: period.name,
      startHour: period.startHour,
      startMinute: period.startMinute,
      endHour: period.endHour,
      endMinute: period.endMinute,
      daysOfWeek: period.daysOfWeek,
      autoOpenEnabled: period.autoOpenEnabled,
      autoCloseEnabled: period.autoCloseEnabled,
      isActive: period.isActive,
      timezone: period.timezone
    });
    setPeriodDialogOpen(true);
  };

  const openNewDialog = () => {
    setEditingPeriod(null);
    resetPeriodForm();
    setPeriodDialogOpen(true);
  };

  const handleSavePeriod = () => {
    if (!periodForm.name?.trim()) {
      toast({
        title: "❌ Error",
        description: "El nombre del período es requerido",
        variant: "destructive",
      });
      return;
    }

    if (periodForm.startHour === periodForm.endHour && periodForm.startMinute === periodForm.endMinute) {
      toast({
        title: "❌ Error",
        description: "La hora de inicio y fin no pueden ser iguales",
        variant: "destructive",
      });
      return;
    }

    savePeriodMutation.mutate(periodForm);
  };

  const formatTime = (hour: number, minute: number) => {
    return `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}`;
  };

  const formatDays = (daysOfWeek: string) => {
    const days = daysOfWeek.split(',');
    return days.map(day => DAYS_OF_WEEK.find(d => d.value === day)?.label).join(', ');
  };

  const toggleDaySelection = (dayValue: string) => {
    const currentDays = periodForm.daysOfWeek?.split(',') || [];
    const newDays = currentDays.includes(dayValue)
      ? currentDays.filter(d => d !== dayValue)
      : [...currentDays, dayValue].sort();
    
    setPeriodForm(prev => ({ ...prev, daysOfWeek: newDays.join(',') }));
  };

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header
        onMobileNavToggle={() => setMobileNavOpen(!mobileNavOpen)}
        isMobileNavOpen={mobileNavOpen}
      />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar 
          isOpen={!mobileNavOpen} 
          userRole={user?.role || ''} 
        />
        <MobileNav 
          isOpen={mobileNavOpen} 
          onClose={() => setMobileNavOpen(false)}
          userRole={user?.role || ''}
        />

        <main className="flex-1 overflow-auto bg-background">
          <div className="container mx-auto px-4 py-6">
            <div className="mb-6">
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white flex items-center">
                <Clock className="mr-3 h-6 w-6 text-blue-600" />
                Configuración de Horarios Múltiples
              </h1>
              <p className="text-gray-600 dark:text-gray-400">
                Configura múltiples períodos de apertura y cierre automático por día
              </p>
            </div>

            {/* Service Status Card */}
            <Card className="mb-6 border-l-4 border-l-blue-500">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center">
                    <Activity className="mr-2 h-5 w-5" />
                    Estado del Servicio Automático
                  </div>
                  <Badge variant={serviceStatus?.isRunning ? "default" : "destructive"}>
                    {serviceStatus?.isRunning ? "🟢 Activo" : "🔴 Inactivo"}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg">
                    <div className="flex items-center space-x-2">
                      <CheckCircle className="h-5 w-5 text-green-600" />
                      <span className="font-medium">Estado:</span>
                    </div>
                    <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                      {serviceStatus?.isRunning ? "Servicio ejecutándose" : "Servicio detenido"}
                    </p>
                  </div>
                  <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
                    <div className="flex items-center space-x-2">
                      <Clock3 className="h-5 w-5 text-blue-600" />
                      <span className="font-medium">Última verificación:</span>
                    </div>
                    <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                      {serviceStatus?.lastCheck ? new Date(serviceStatus.lastCheck).toLocaleString() : "N/A"}
                    </p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-900/20 p-4 rounded-lg">
                    <div className="flex items-center space-x-2">
                      <Zap className="h-5 w-5 text-purple-600" />
                      <span className="font-medium">Tiempo activo:</span>
                    </div>
                    <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                      {serviceStatus?.uptime ? `${Math.floor(serviceStatus.uptime / 1000 / 60)} min` : "N/A"}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Periods Management */}
            <Card className="mb-6">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center">
                    <Calendar className="mr-2 h-5 w-5" />
                    Períodos Configurados
                  </div>
                  <Button onClick={openNewDialog} size="sm">
                    <Plus className="mr-2 h-4 w-4" />
                    Agregar Período
                  </Button>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {periodsLoading ? (
                  <div className="text-center py-8">
                    <RefreshCw className="h-8 w-8 mx-auto mb-2 animate-spin" />
                    <p>Cargando períodos...</p>
                  </div>
                ) : periods.length > 0 ? (
                  <div className="space-y-4">
                    {periods.map((period: SchedulePeriod) => (
                      <div key={period.id} className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                        <div className="flex items-center space-x-4">
                          <div className="text-2xl">
                            {period.isActive ? '🟢' : '🔴'}
                          </div>
                          <div>
                            <h4 className="font-medium">{period.name}</h4>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                              {formatTime(period.startHour, period.startMinute)} - {formatTime(period.endHour, period.endMinute)}
                            </p>
                            <p className="text-xs text-gray-500">
                              {formatDays(period.daysOfWeek)}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center space-x-2">
                          <Badge variant={period.autoOpenEnabled ? "default" : "secondary"}>
                            {period.autoOpenEnabled ? 'Auto-Abrir' : 'Sin Auto-Abrir'}
                          </Badge>
                          <Badge variant={period.autoCloseEnabled ? "default" : "secondary"}>
                            {period.autoCloseEnabled ? 'Auto-Cerrar' : 'Sin Auto-Cerrar'}
                          </Badge>
                          <Button 
                            variant="outline" 
                            size="sm"
                            onClick={() => openEditDialog(period)}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button variant="outline" size="sm">
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                              <AlertDialogHeader>
                                <AlertDialogTitle>¿Eliminar período?</AlertDialogTitle>
                                <AlertDialogDescription>
                                  Esta acción eliminará permanentemente el período "{period.name}".
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel>Cancelar</AlertDialogCancel>
                                <AlertDialogAction 
                                  onClick={() => deletePeriodMutation.mutate(period.id!)}
                                  className="bg-red-600 hover:bg-red-700"
                                >
                                  Eliminar
                                </AlertDialogAction>
                              </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                    <Calendar className="h-8 w-8 mx-auto mb-2 opacity-50" />
                    <p>No hay períodos configurados</p>
                    <p className="text-sm">Agrega tu primer período de horarios</p>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Next Operations Preview */}
            <Card className="mb-6">
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Eye className="mr-2 h-5 w-5" />
                  Próximas Operaciones Programadas
                </CardTitle>
              </CardHeader>
              <CardContent>
                {nextOperations.length > 0 ? (
                  <div className="space-y-3">
                    {nextOperations.map((operation: any, index: number) => (
                      <div key={index} className="flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                        <div className="flex items-center space-x-3">
                          <div className="text-xl">
                            {operation.type === 'auto_open' ? '🌅' : '🌆'}
                          </div>
                          <div>
                            <h4 className="font-medium">
                              {operation.type === 'auto_open' ? 'Apertura' : 'Cierre'} - {operation.periodName}
                            </h4>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                              {new Date(operation.scheduledTime).toLocaleString('es-AR', {
                                timeZone: 'America/Argentina/Buenos_Aires',
                                weekday: 'long',
                                year: 'numeric',
                                month: 'long',
                                day: 'numeric',
                                hour: '2-digit',
                                minute: '2-digit'
                              })}
                            </p>
                          </div>
                        </div>
                        <Badge variant="outline">
                          {operation.status || 'Programado'}
                        </Badge>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                    <Clock className="h-8 w-8 mx-auto mb-2 opacity-50" />
                    <p>No hay operaciones programadas</p>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Operations Log */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Activity className="mr-2 h-5 w-5" />
                  Historial de Operaciones
                </CardTitle>
              </CardHeader>
              <CardContent>
                {operationsLog.length > 0 ? (
                  <div className="space-y-3">
                    {operationsLog.map((log: OperationLog) => (
                      <div key={log.id} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                        <div className="flex items-center space-x-3">
                          <div className="text-xl">
                            {log.type === 'auto_open' ? '🌅' : '🌆'}
                          </div>
                          <div>
                            <h4 className="font-medium">
                              {log.type === 'auto_open' ? 'Apertura' : 'Cierre'} - {log.periodName}
                            </h4>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                              Ejecutado: {new Date(log.executedTime).toLocaleString('es-AR', {
                                timeZone: 'America/Argentina/Buenos_Aires'
                              })}
                            </p>
                            {log.errorMessage && (
                              <p className="text-xs text-red-600">{log.errorMessage}</p>
                            )}
                          </div>
                        </div>
                        <Badge variant={log.status === 'success' ? 'default' : log.status === 'failed' ? 'destructive' : 'secondary'}>
                          {log.status === 'success' ? '✅ Éxito' : log.status === 'failed' ? '❌ Error' : '⏳ Pendiente'}
                        </Badge>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                    <Activity className="h-8 w-8 mx-auto mb-2 opacity-50" />
                    <p>No hay operaciones registradas aún</p>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </main>
      </div>

      {/* Period Dialog */}
      <Dialog open={periodDialogOpen} onOpenChange={setPeriodDialogOpen}>
        <DialogContent className="sm:max-w-[600px]">
          <DialogHeader>
            <DialogTitle>
              {editingPeriod ? 'Editar Período' : 'Nuevo Período'}
            </DialogTitle>
            <DialogDescription>
              Configura los horarios de apertura y cierre automático
            </DialogDescription>
          </DialogHeader>
          
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="name">Nombre del Período</Label>
              <Input
                id="name"
                value={periodForm.name || ''}
                onChange={(e) => setPeriodForm(prev => ({ ...prev, name: e.target.value }))}
                placeholder="Ej: Horario Mañana, Turno Tarde..."
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-2">
                <Label>Hora de Inicio</Label>
                <div className="flex space-x-2">
                  <Select 
                    value={periodForm.startHour?.toString()} 
                    onValueChange={(value) => setPeriodForm(prev => ({ ...prev, startHour: parseInt(value) }))}
                  >
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {Array.from({length: 24}, (_, i) => (
                        <SelectItem key={i} value={i.toString()}>
                          {i.toString().padStart(2, '0')}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select 
                    value={periodForm.startMinute?.toString()} 
                    onValueChange={(value) => setPeriodForm(prev => ({ ...prev, startMinute: parseInt(value) }))}
                  >
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {[0, 15, 30, 45].map(minute => (
                        <SelectItem key={minute} value={minute.toString()}>
                          {minute.toString().padStart(2, '0')}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid gap-2">
                <Label>Hora de Fin</Label>
                <div className="flex space-x-2">
                  <Select 
                    value={periodForm.endHour?.toString()} 
                    onValueChange={(value) => setPeriodForm(prev => ({ ...prev, endHour: parseInt(value) }))}
                  >
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {Array.from({length: 24}, (_, i) => (
                        <SelectItem key={i} value={i.toString()}>
                          {i.toString().padStart(2, '0')}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select 
                    value={periodForm.endMinute?.toString()} 
                    onValueChange={(value) => setPeriodForm(prev => ({ ...prev, endMinute: parseInt(value) }))}
                  >
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {[0, 15, 30, 45].map(minute => (
                        <SelectItem key={minute} value={minute.toString()}>
                          {minute.toString().padStart(2, '0')}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            <div className="grid gap-2">
              <Label>Días de la Semana</Label>
              <div className="flex flex-wrap gap-2">
                {DAYS_OF_WEEK.map(day => (
                  <Button
                    key={day.value}
                    type="button"
                    variant={periodForm.daysOfWeek?.includes(day.value) ? "default" : "outline"}
                    size="sm"
                    onClick={() => toggleDaySelection(day.value)}
                  >
                    {day.label}
                  </Button>
                ))}
              </div>
            </div>

            <div className="grid gap-4">
              <div className="flex items-center justify-between">
                <Label htmlFor="auto-open">Apertura Automática</Label>
                <Switch
                  id="auto-open"
                  checked={periodForm.autoOpenEnabled}
                  onCheckedChange={(checked) => setPeriodForm(prev => ({ ...prev, autoOpenEnabled: checked }))}
                />
              </div>

              <div className="flex items-center justify-between">
                <Label htmlFor="auto-close">Cierre Automático</Label>
                <Switch
                  id="auto-close"
                  checked={periodForm.autoCloseEnabled}
                  onCheckedChange={(checked) => setPeriodForm(prev => ({ ...prev, autoCloseEnabled: checked }))}
                />
              </div>

              <div className="flex items-center justify-between">
                <Label htmlFor="is-active">Período Activo</Label>
                <Switch
                  id="is-active"
                  checked={periodForm.isActive}
                  onCheckedChange={(checked) => setPeriodForm(prev => ({ ...prev, isActive: checked }))}
                />
              </div>
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setPeriodDialogOpen(false)}>
              Cancelar
            </Button>
            <Button onClick={handleSavePeriod} disabled={savePeriodMutation.isPending}>
              {savePeriodMutation.isPending ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Guardando...
                </>
              ) : (
                <>
                  <Save className="mr-2 h-4 w-4" />
                  {editingPeriod ? 'Actualizar' : 'Crear'} Período
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Footer />
    </div>
  );
}
