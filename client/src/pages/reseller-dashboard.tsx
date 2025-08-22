import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import { 
  Loader2, 
  Plus, 
  DollarSign, 
  TrendingUp, 
  Users, 
  Settings,
  Calendar,
  Target
} from "lucide-react";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import { apiRequest } from "@/lib/queryClient";

interface ResellerStats {
  totalSales: number;
  monthlyRevenue: number;
  monthlyProfit: number;
  monthlyCommission: number;
  accountsQuota: number;
  accountsSold: number;
}

interface ResellerSale {
  id: number;
  resellerId: number;
  clientId: number;
  clientName?: string;
  clientEmail?: string;
  purchasePrice: string;
  salePrice: string;
  profit: string;
  commission: string;
  subscriptionType: string;
  trialDays: number;
  saleDate: string;
  notes?: string;
  createdAt: string;
}

interface ResellerConfiguration {
  id: number;
  resellerId: number;
  premiumMonthlyPrice: string;
  premiumYearlyPrice: string;
  premiumYearlyDiscount: string;
  defaultTrialDays: number;
  supportMessage: string;
  createdAt: string;
  updatedAt?: string;
}

export default function ResellerDashboard() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [isCreateSaleDialogOpen, setIsCreateSaleDialogOpen] = useState(false);
  const [isConfigDialogOpen, setIsConfigDialogOpen] = useState(false);
  const [saleFormData, setSaleFormData] = useState({
    clientName: "",
    clientEmail: "",
    purchasePrice: 29.99,
    salePrice: 39.99,
    subscriptionType: "premium_monthly",
    trialDays: 7,
    notes: "",
  });

  // Queries
  const { data: stats, isLoading: statsLoading } = useQuery<ResellerStats>({
    queryKey: ["/api/reseller/stats"],
    queryFn: async () => {
      const response = await fetch("/api/reseller/stats", {
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
      if (!response.ok) throw new Error("Failed to fetch stats");
      return response.json();
    },
    enabled: !!user?.id && user?.role === "reseller",
  });

  const { data: sales, isLoading: salesLoading } = useQuery<ResellerSale[]>({
    queryKey: ["/api/reseller/sales"],
    queryFn: async () => {
      const response = await fetch("/api/reseller/sales", {
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
      if (!response.ok) throw new Error("Failed to fetch sales");
      return response.json();
    },
    enabled: !!user?.id && user?.role === "reseller",
  });

  const { data: configuration } = useQuery<ResellerConfiguration>({
    queryKey: ["/api/reseller/configuration"],
    queryFn: async () => {
      const response = await fetch("/api/reseller/configuration", {
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
      if (!response.ok) throw new Error("Failed to fetch configuration");
      return response.json();
    },
    enabled: !!user?.id && user?.role === "reseller",
  });

  // Mutations
  const createSaleMutation = useMutation({
    mutationFn: async (data: any) => {
      return apiRequest("/api/reseller/sales", {
        method: "POST",
        body: JSON.stringify(data),
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/reseller/sales"] });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller/stats"] });
      setIsCreateSaleDialogOpen(false);
      setSaleFormData({
        clientName: "",
        clientEmail: "",
        purchasePrice: 29.99,
        salePrice: 39.99,
        subscriptionType: "premium_monthly",
        trialDays: 7,
        notes: "",
      });
      toast({
        title: "Venta registrada",
        description: "La venta ha sido registrada exitosamente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al registrar venta",
        variant: "destructive",
      });
    },
  });

  const updateConfigurationMutation = useMutation({
    mutationFn: async (data: any) => {
      return apiRequest("/api/reseller/configuration", {
        method: "PUT",
        body: JSON.stringify(data),
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/reseller/configuration"] });
      setIsConfigDialogOpen(false);
      toast({
        title: "Configuración actualizada",
        description: "La configuración ha sido actualizada correctamente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al actualizar configuración",
        variant: "destructive",
      });
    },
  });

  const handleCreateSale = () => {
    createSaleMutation.mutate(saleFormData);
  };

  const handleUpdateConfiguration = () => {
    if (configuration) {
      updateConfigurationMutation.mutate({
        premiumMonthlyPrice: configuration.premiumMonthlyPrice,
        premiumYearlyPrice: configuration.premiumYearlyPrice,
        premiumYearlyDiscount: configuration.premiumYearlyDiscount,
        defaultTrialDays: configuration.defaultTrialDays,
        supportMessage: configuration.supportMessage,
      });
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString("es-ES", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  };

  const getSubscriptionTypeLabel = (type: string) => {
    const types: Record<string, { label: string; color: string }> = {
      trial: { label: "Prueba", color: "bg-blue-100 text-blue-800" },
      premium_monthly: { label: "Premium Mensual", color: "bg-green-100 text-green-800" },
      premium_yearly: { label: "Premium Anual", color: "bg-purple-100 text-purple-800" },
      unlimited: { label: "Ilimitado", color: "bg-yellow-100 text-yellow-800" },
    };
    return types[type] || { label: type, color: "bg-gray-100 text-gray-800" };
  };

  if (user?.role !== "reseller") {
    return (
      <div className="flex h-screen bg-gray-100 dark:bg-gray-900">
        <Sidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          <Header />
          <main className="flex-1 overflow-x-hidden overflow-y-auto bg-gray-100 dark:bg-gray-900 p-6">
            <div className="text-center py-8">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                Acceso Denegado
              </h2>
              <p className="text-gray-600 dark:text-gray-400 mt-2">
                Solo los revendedores pueden acceder a este panel.
              </p>
            </div>
          </main>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-gray-100 dark:bg-gray-900">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Header />
        <main className="flex-1 overflow-x-hidden overflow-y-auto bg-gray-100 dark:bg-gray-900 p-6">
          <div className="max-w-7xl mx-auto">
            <div className="mb-6">
              <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
                Dashboard del Revendedor
              </h1>
              <p className="text-gray-600 dark:text-gray-400 mt-2">
                Gestiona tus ventas, clientes y configuración
              </p>
            </div>

            {/* Statistics Cards */}
            {statsLoading ? (
              <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                {[...Array(4)].map((_, i) => (
                  <Card key={i}>
                    <CardContent className="p-6">
                      <div className="animate-pulse">
                        <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                        <div className="h-8 bg-gray-200 rounded w-1/2 mb-2"></div>
                        <div className="h-3 bg-gray-200 rounded w-full"></div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Cuota de Cuentas</CardTitle>
                    <Target className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      {stats?.accountsSold || 0}/{stats?.accountsQuota || 0}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {((stats?.accountsSold || 0) / (stats?.accountsQuota || 1) * 100).toFixed(1)}% utilizado
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Ingresos del Mes</CardTitle>
                    <DollarSign className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      ${(stats?.monthlyRevenue || 0).toFixed(2)}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Facturación mensual
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Ganancia del Mes</CardTitle>
                    <TrendingUp className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      ${(stats?.monthlyProfit || 0).toFixed(2)}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Ganancia bruta mensual
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Comisión del Mes</CardTitle>
                    <Users className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      ${(stats?.monthlyCommission || 0).toFixed(2)}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Tu comisión mensual
                    </p>
                  </CardContent>
                </Card>
              </div>
            )}

            {/* Action Buttons */}
            <div className="flex gap-4 mb-6">
              <Dialog open={isCreateSaleDialogOpen} onOpenChange={setIsCreateSaleDialogOpen}>
                <DialogTrigger asChild>
                  <Button>
                    <Plus className="mr-2 h-4 w-4" />
                    Registrar Venta
                  </Button>
                </DialogTrigger>
                <DialogContent className="sm:max-w-[600px]">
                  <DialogHeader>
                    <DialogTitle>Registrar Nueva Venta</DialogTitle>
                  </DialogHeader>
                  <div className="grid gap-4 py-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="clientName">Nombre del Cliente</Label>
                        <Input
                          id="clientName"
                          value={saleFormData.clientName}
                          onChange={(e) => setSaleFormData(prev => ({ ...prev, clientName: e.target.value }))}
                          placeholder="Empresa o nombre completo"
                        />
                      </div>
                      <div>
                        <Label htmlFor="clientEmail">Email del Cliente</Label>
                        <Input
                          id="clientEmail"
                          type="email"
                          value={saleFormData.clientEmail}
                          onChange={(e) => setSaleFormData(prev => ({ ...prev, clientEmail: e.target.value }))}
                          placeholder="cliente@empresa.com"
                        />
                      </div>
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="purchasePrice">Precio de Compra ($)</Label>
                        <Input
                          id="purchasePrice"
                          type="number"
                          step="0.01"
                          value={saleFormData.purchasePrice}
                          onChange={(e) => setSaleFormData(prev => ({ ...prev, purchasePrice: parseFloat(e.target.value) }))}
                        />
                      </div>
                      <div>
                        <Label htmlFor="salePrice">Precio de Venta ($)</Label>
                        <Input
                          id="salePrice"
                          type="number"
                          step="0.01"
                          value={saleFormData.salePrice}
                          onChange={(e) => setSaleFormData(prev => ({ ...prev, salePrice: parseFloat(e.target.value) }))}
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="subscriptionType">Tipo de Suscripción</Label>
                        <Select 
                          value={saleFormData.subscriptionType} 
                          onValueChange={(value) => setSaleFormData(prev => ({ ...prev, subscriptionType: value }))}
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="trial">Prueba</SelectItem>
                            <SelectItem value="premium_monthly">Premium Mensual</SelectItem>
                            <SelectItem value="premium_yearly">Premium Anual</SelectItem>
                            <SelectItem value="unlimited">Ilimitado</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <Label htmlFor="trialDays">Días de Prueba</Label>
                        <Input
                          id="trialDays"
                          type="number"
                          value={saleFormData.trialDays}
                          onChange={(e) => setSaleFormData(prev => ({ ...prev, trialDays: parseInt(e.target.value) }))}
                        />
                      </div>
                    </div>

                    <div>
                      <Label htmlFor="notes">Notas (opcional)</Label>
                      <Textarea
                        id="notes"
                        value={saleFormData.notes}
                        onChange={(e) => setSaleFormData(prev => ({ ...prev, notes: e.target.value }))}
                        placeholder="Información adicional sobre la venta..."
                        rows={3}
                      />
                    </div>

                    {/* Profit Calculation */}
                    <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
                      <h4 className="font-medium mb-2">Resumen de Ganancia</h4>
                      <div className="grid grid-cols-3 gap-4 text-sm">
                        <div>
                          <span className="text-muted-foreground">Ganancia:</span>
                          <p className="font-medium">
                            ${(saleFormData.salePrice - saleFormData.purchasePrice).toFixed(2)}
                          </p>
                        </div>
                        <div>
                          <span className="text-muted-foreground">Comisión (10%):</span>
                          <p className="font-medium">
                            ${((saleFormData.salePrice - saleFormData.purchasePrice) * 0.1).toFixed(2)}
                          </p>
                        </div>
                        <div>
                          <span className="text-muted-foreground">Margen:</span>
                          <p className="font-medium">
                            {(((saleFormData.salePrice - saleFormData.purchasePrice) / saleFormData.salePrice) * 100).toFixed(1)}%
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="flex justify-end gap-2">
                    <Button variant="outline" onClick={() => setIsCreateSaleDialogOpen(false)}>
                      Cancelar
                    </Button>
                    <Button 
                      onClick={handleCreateSale}
                      disabled={createSaleMutation.isPending}
                    >
                      {createSaleMutation.isPending && (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      )}
                      Registrar Venta
                    </Button>
                  </div>
                </DialogContent>
              </Dialog>

              <Dialog open={isConfigDialogOpen} onOpenChange={setIsConfigDialogOpen}>
                <DialogTrigger asChild>
                  <Button variant="outline">
                    <Settings className="mr-2 h-4 w-4" />
                    Configuración
                  </Button>
                </DialogTrigger>
                <DialogContent className="sm:max-w-[500px]">
                  <DialogHeader>
                    <DialogTitle>Configuración de Precios</DialogTitle>
                  </DialogHeader>
                  {configuration && (
                    <div className="grid gap-4 py-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <Label htmlFor="monthlyPrice">Precio Premium Mensual</Label>
                          <Input
                            id="monthlyPrice"
                            value={configuration.premiumMonthlyPrice}
                            onChange={(e) => {
                              queryClient.setQueryData(["/api/reseller/configuration"], {
                                ...configuration,
                                premiumMonthlyPrice: e.target.value
                              });
                            }}
                          />
                        </div>
                        <div>
                          <Label htmlFor="yearlyPrice">Precio Premium Anual</Label>
                          <Input
                            id="yearlyPrice"
                            value={configuration.premiumYearlyPrice}
                            onChange={(e) => {
                              queryClient.setQueryData(["/api/reseller/configuration"], {
                                ...configuration,
                                premiumYearlyPrice: e.target.value
                              });
                            }}
                          />
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <Label htmlFor="yearlyDiscount">Descuento Anual</Label>
                          <Input
                            id="yearlyDiscount"
                            value={configuration.premiumYearlyDiscount}
                            onChange={(e) => {
                              queryClient.setQueryData(["/api/reseller/configuration"], {
                                ...configuration,
                                premiumYearlyDiscount: e.target.value
                              });
                            }}
                          />
                        </div>
                        <div>
                          <Label htmlFor="defaultTrialDays">Días de Prueba por Defecto</Label>
                          <Input
                            id="defaultTrialDays"
                            type="number"
                            value={configuration.defaultTrialDays}
                            onChange={(e) => {
                              queryClient.setQueryData(["/api/reseller/configuration"], {
                                ...configuration,
                                defaultTrialDays: parseInt(e.target.value)
                              });
                            }}
                          />
                        </div>
                      </div>
                      <div>
                        <Label htmlFor="supportMessage">Mensaje de Soporte</Label>
                        <Textarea
                          id="supportMessage"
                          value={configuration.supportMessage}
                          onChange={(e) => {
                            queryClient.setQueryData(["/api/reseller/configuration"], {
                              ...configuration,
                              supportMessage: e.target.value
                            });
                          }}
                          rows={3}
                        />
                      </div>
                    </div>
                  )}
                  <div className="flex justify-end gap-2">
                    <Button variant="outline" onClick={() => setIsConfigDialogOpen(false)}>
                      Cancelar
                    </Button>
                    <Button 
                      onClick={handleUpdateConfiguration}
                      disabled={updateConfigurationMutation.isPending}
                    >
                      {updateConfigurationMutation.isPending && (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      )}
                      Guardar Configuración
                    </Button>
                  </div>
                </DialogContent>
              </Dialog>
            </div>

            {/* Sales Table */}
            <Card>
              <CardHeader>
                <CardTitle>Historial de Ventas</CardTitle>
              </CardHeader>
              <CardContent>
                {salesLoading ? (
                  <div className="flex justify-center items-center py-8">
                    <Loader2 className="h-8 w-8 animate-spin" />
                  </div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Fecha</TableHead>
                        <TableHead>Cliente</TableHead>
                        <TableHead>Tipo</TableHead>
                        <TableHead>Precio Compra</TableHead>
                        <TableHead>Precio Venta</TableHead>
                        <TableHead>Ganancia</TableHead>
                        <TableHead>Comisión</TableHead>
                        <TableHead>Prueba</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sales?.map((sale) => {
                        const subType = getSubscriptionTypeLabel(sale.subscriptionType);
                        return (
                          <TableRow key={sale.id}>
                            <TableCell>{formatDate(sale.saleDate)}</TableCell>
                            <TableCell>
                              <div>
                                <div className="font-medium">{sale.clientName}</div>
                                <div className="text-sm text-muted-foreground">{sale.clientEmail}</div>
                              </div>
                            </TableCell>
                            <TableCell>
                              <Badge className={subType.color}>
                                {subType.label}
                              </Badge>
                            </TableCell>
                            <TableCell>${parseFloat(sale.purchasePrice).toFixed(2)}</TableCell>
                            <TableCell>${parseFloat(sale.salePrice).toFixed(2)}</TableCell>
                            <TableCell>${parseFloat(sale.profit).toFixed(2)}</TableCell>
                            <TableCell>${parseFloat(sale.commission).toFixed(2)}</TableCell>
                            <TableCell>{sale.trialDays} días</TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </div>
        </main>
      </div>
    </div>
  );
}