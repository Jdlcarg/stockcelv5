import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { useAuth } from "@/lib/auth";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { CreditCard, DollarSign, TrendingUp, Lock, Unlock, Calculator } from "lucide-react";

interface CashRegister {
  id: number;
  clientId: number;
  date: string;
  initialUsd: string;
  initialArs: string;
  initialUsdt: string;
  currentUsd: string;
  currentArs: string;
  currentUsdt: string;
  dailySales: string;
  isOpen: boolean;
  closedAt?: string;
  createdAt: string;
}

export default function Cash() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [openCashModalOpen, setOpenCashModalOpen] = useState(false);
  const [closeCashModalOpen, setCloseCashModalOpen] = useState(false);
  const [initialAmounts, setInitialAmounts] = useState({
    usd: "",
    ars: "",
    usdt: "",
  });
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Obtener estado actual de la caja
  const { data: cashRegister, isLoading } = useQuery({
    queryKey: ["/api/cash-register", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-register/current?clientId=${user?.clientId}`);
      if (!response.ok) {
        if (response.status === 404) {
          return null; // No hay caja abierta
        }
        throw new Error("Error al obtener estado de caja");
      }
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Obtener estado en tiempo real de la caja
  const { data: realTimeState } = useQuery({
    queryKey: ["/api/cash-register/real-time-state", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-register/real-time-state?clientId=${user?.clientId}`);
      if (!response.ok) {
        throw new Error("Error al obtener estado en tiempo real");
      }
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: 30000, // Actualizar cada 30 segundos
  });

  // Mutación para abrir caja
  const openCashMutation = useMutation({
    mutationFn: async (amounts: { usd: string; ars: string; usdt: string }) => {
      return apiRequest('POST', '/api/cash-register/open', {
        clientId: user?.clientId,
        initialUsd: amounts.usd,
        initialArs: amounts.ars,
        initialUsdt: amounts.usdt,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/cash-register"] });
      setOpenCashModalOpen(false);
      setInitialAmounts({ usd: "", ars: "", usdt: "" });
      toast({
        title: "Éxito",
        description: "Caja abierta correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo abrir la caja",
        variant: "destructive",
      });
    },
  });

  // Mutación para cerrar caja
  const closeCashMutation = useMutation({
    mutationFn: async () => {
      return apiRequest('POST', '/api/cash-register/close', {
        clientId: user?.clientId,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/cash-register"] });
      setCloseCashModalOpen(false);
      toast({
        title: "Éxito",
        description: "Caja cerrada correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo cerrar la caja",
        variant: "destructive",
      });
    },
  });

  const handleOpenCash = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!initialAmounts.usd || !initialAmounts.ars || !initialAmounts.usdt) {
      toast({
        title: "Error",
        description: "Todos los campos son requeridos",
        variant: "destructive",
      });
      return;
    }

    const usdAmount = parseFloat(initialAmounts.usd);
    const arsAmount = parseFloat(initialAmounts.ars);
    const usdtAmount = parseFloat(initialAmounts.usdt);

    if (isNaN(usdAmount) || isNaN(arsAmount) || isNaN(usdtAmount)) {
      toast({
        title: "Error",
        description: "Los montos deben ser números válidos",
        variant: "destructive",
      });
      return;
    }

    if (usdAmount < 0 || arsAmount < 0 || usdtAmount < 0) {
      toast({
        title: "Error",
        description: "Los montos no pueden ser negativos",
        variant: "destructive",
      });
      return;
    }

    openCashMutation.mutate(initialAmounts);
  };

  const handleCloseCash = () => {
    closeCashMutation.mutate();
  };

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
          <div className="px-4 sm:px-6 lg:px-8 py-8">
            <div className="flex items-center justify-between mb-8">
              <h1 className="text-2xl font-bold text-gray-900">Gestión de Caja</h1>
              
              {!cashRegister ? (
                <Dialog open={openCashModalOpen} onOpenChange={setOpenCashModalOpen}>
                  <DialogTrigger asChild>
                    <Button>
                      <Unlock className="mr-2 h-4 w-4" />
                      Abrir Caja
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Abrir Caja</DialogTitle>
                    </DialogHeader>
                    <form onSubmit={handleOpenCash} className="space-y-4">
                      <div>
                        <Label htmlFor="initial-usd">Monto Inicial USD</Label>
                        <Input
                          id="initial-usd"
                          type="number"
                          step="0.01"
                          value={initialAmounts.usd}
                          onChange={(e) => setInitialAmounts({ ...initialAmounts, usd: e.target.value })}
                          placeholder="0.00"
                          required
                        />
                      </div>
                      <div>
                        <Label htmlFor="initial-ars">Monto Inicial ARS</Label>
                        <Input
                          id="initial-ars"
                          type="number"
                          step="0.01"
                          value={initialAmounts.ars}
                          onChange={(e) => setInitialAmounts({ ...initialAmounts, ars: e.target.value })}
                          placeholder="0.00"
                          required
                        />
                      </div>
                      <div>
                        <Label htmlFor="initial-usdt">Monto Inicial USDT</Label>
                        <Input
                          id="initial-usdt"
                          type="number"
                          step="0.01"
                          value={initialAmounts.usdt}
                          onChange={(e) => setInitialAmounts({ ...initialAmounts, usdt: e.target.value })}
                          placeholder="0.00"
                          required
                        />
                      </div>
                      <div className="flex justify-end space-x-2">
                        <Button type="button" variant="outline" onClick={() => setOpenCashModalOpen(false)}>
                          Cancelar
                        </Button>
                        <Button type="submit" disabled={openCashMutation.isPending}>
                          {openCashMutation.isPending ? "Abriendo..." : "Abrir Caja"}
                        </Button>
                      </div>
                    </form>
                  </DialogContent>
                </Dialog>
              ) : (
                <div className="flex items-center space-x-4">
                  <Badge variant="secondary" className="bg-green-100 text-green-800">
                    <Unlock className="mr-1 h-3 w-3" />
                    Caja Abierta
                  </Badge>
                  <Dialog open={closeCashModalOpen} onOpenChange={setCloseCashModalOpen}>
                    <DialogTrigger asChild>
                      <Button variant="outline">
                        <Lock className="mr-2 h-4 w-4" />
                        Cerrar Caja
                      </Button>
                    </DialogTrigger>
                    <DialogContent>
                      <DialogHeader>
                        <DialogTitle>Cerrar Caja</DialogTitle>
                      </DialogHeader>
                      <div className="space-y-4">
                        <p className="text-sm text-gray-600">
                          ¿Estás seguro que deseas cerrar la caja? Esta acción no se puede deshacer.
                        </p>
                        <div className="flex justify-end space-x-2">
                          <Button type="button" variant="outline" onClick={() => setCloseCashModalOpen(false)}>
                            Cancelar
                          </Button>
                          <Button onClick={handleCloseCash} disabled={closeCashMutation.isPending}>
                            {closeCashMutation.isPending ? "Cerrando..." : "Cerrar Caja"}
                          </Button>
                        </div>
                      </div>
                    </DialogContent>
                  </Dialog>
                </div>
              )}
            </div>

            {isLoading ? (
              <div className="flex items-center justify-center h-64">
                <div className="text-gray-500">Cargando...</div>
              </div>
            ) : (
              <>
                {/* Mostrar resumen de estado en tiempo real siempre */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
                  {/* Balance Total */}
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center">
                        <DollarSign className="mr-2 h-5 w-5" />
                        Balance Total
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold text-blue-600">
                        ${realTimeState ? parseFloat(realTimeState.totalBalanceUsd).toFixed(2) : '0.00'}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Balance actual en USD
                      </p>
                    </CardContent>
                  </Card>

                  {/* Ventas del Día */}
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center">
                        <TrendingUp className="mr-2 h-5 w-5" />
                        Ventas del Día
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold text-green-600">
                        ${realTimeState ? parseFloat(realTimeState.dailySalesUsd).toFixed(2) : '0.00'}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Total ventas hoy
                      </p>
                    </CardContent>
                  </Card>

                  {/* Gastos del Día */}
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center">
                        <CreditCard className="mr-2 h-5 w-5" />
                        Gastos del Día
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold text-red-600">
                        ${realTimeState ? parseFloat(realTimeState.dailyExpensesUsd).toFixed(2) : '0.00'}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Total gastos hoy
                      </p>
                    </CardContent>
                  </Card>

                  {/* Deudas Activas */}
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center">
                        <Calculator className="mr-2 h-5 w-5" />
                        Deudas Activas
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold text-orange-600">
                        ${realTimeState ? parseFloat(realTimeState.totalActiveDebtsUsd).toFixed(2) : '0.00'}
                      </div>
                      <p className="text-sm text-gray-600 mt-1">
                        Total deudas pendientes
                      </p>
                    </CardContent>
                  </Card>
                </div>

                {!cashRegister ? (
                  <Card>
                    <CardContent className="text-center py-12">
                      <Lock className="mx-auto h-12 w-12 text-gray-400 mb-4" />
                      <h3 className="text-lg font-medium text-gray-900 mb-2">Caja Cerrada</h3>
                      <p className="text-gray-600 mb-4">
                        Los movimientos de caja se registran automáticamente. Puedes abrir una caja para gestión manual.
                      </p>
                      <Button onClick={() => setOpenCashModalOpen(true)}>
                        <Unlock className="mr-2 h-4 w-4" />
                        Abrir Caja Manual
                      </Button>
                    </CardContent>
                  </Card>
                ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {/* Montos Iniciales */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center">
                      <DollarSign className="mr-2 h-5 w-5" />
                      Montos Iniciales
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex justify-between">
                      <span className="text-sm font-medium">USD:</span>
                      <span className="text-sm">${parseFloat(cashRegister.initialUsd).toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-sm font-medium">ARS:</span>
                      <span className="text-sm">${parseFloat(cashRegister.initialArs).toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-sm font-medium">USDT:</span>
                      <span className="text-sm">${parseFloat(cashRegister.initialUsdt).toFixed(2)}</span>
                    </div>
                  </CardContent>
                </Card>

                {/* Montos Actuales */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center">
                      <Calculator className="mr-2 h-5 w-5" />
                      Montos Actuales
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex justify-between">
                      <span className="text-sm font-medium">USD:</span>
                      <span className="text-sm font-bold">${parseFloat(cashRegister.currentUsd).toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-sm font-medium">ARS:</span>
                      <span className="text-sm font-bold">${parseFloat(cashRegister.currentArs).toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-sm font-medium">USDT:</span>
                      <span className="text-sm font-bold">${parseFloat(cashRegister.currentUsdt).toFixed(2)}</span>
                    </div>
                  </CardContent>
                </Card>

                {/* Ventas del Día */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center">
                      <TrendingUp className="mr-2 h-5 w-5" />
                      Ventas del Día
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold text-green-600">
                      ${parseFloat(cashRegister.dailySales).toFixed(2)}
                    </div>
                    <p className="text-sm text-gray-600 mt-1">
                      Total en USD
                    </p>
                  </CardContent>
                </Card>

                {/* Resumen de Movimientos */}
                <Card className="md:col-span-2 lg:col-span-3">
                  <CardHeader>
                    <CardTitle>Resumen de Movimientos</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                      <div>
                        <h4 className="font-medium mb-2">Diferencia USD</h4>
                        <div className="text-lg font-bold text-blue-600">
                          ${(parseFloat(cashRegister.currentUsd) - parseFloat(cashRegister.initialUsd)).toFixed(2)}
                        </div>
                      </div>
                      <div>
                        <h4 className="font-medium mb-2">Diferencia ARS</h4>
                        <div className="text-lg font-bold text-blue-600">
                          ${(parseFloat(cashRegister.currentArs) - parseFloat(cashRegister.initialArs)).toFixed(2)}
                        </div>
                      </div>
                      <div>
                        <h4 className="font-medium mb-2">Diferencia USDT</h4>
                        <div className="text-lg font-bold text-blue-600">
                          ${(parseFloat(cashRegister.currentUsdt) - parseFloat(cashRegister.initialUsdt)).toFixed(2)}
                        </div>
                      </div>
                    </div>
                    <Separator className="my-4" />
                    <div className="text-sm text-gray-600">
                      <p>Fecha de apertura: {new Date(cashRegister.date).toLocaleDateString()}</p>
                      <p>Hora de apertura: {new Date(cashRegister.createdAt).toLocaleTimeString()}</p>
                    </div>
                  </CardContent>
                </Card>
                  </div>
                )}
              </>
            )}
          </div>
        </main>
      </div>
      <Footer />
    </div>
  );
}