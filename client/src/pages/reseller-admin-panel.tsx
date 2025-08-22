import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
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
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import { 
  Users, 
  TrendingUp, 
  DollarSign, 
  Plus,
  LogOut,
  Settings,
  Eye,
  Package,
  Edit,
  Trash2
} from "lucide-react";
import { apiRequest } from "@/lib/queryClient";

interface ResellerUser {
  id: number;
  name: string;
  email: string;
  company?: string;
  role: string;
  accountsQuota: number;
  accountsSold: number;
  totalEarnings: string;
}

interface Client {
  id: number;
  name: string;
  email: string;
  subscriptionType: string;
  trialStartDate?: string;
  trialEndDate?: string;
  isActive: boolean;
  createdAt: string;
}

interface ResellerSale {
  id: number;
  clientName: string;
  clientEmail: string;
  salePrice: string;
  profit: string;
  subscriptionType: string;
  saleDate: string;
  notes?: string;
}

export default function ResellerAdminPanel() {
  const [, setLocation] = useLocation();
  const queryClient = useQueryClient();
  const [user, setUser] = useState<ResellerUser | null>(null);
  const [isCreateClientDialogOpen, setIsCreateClientDialogOpen] = useState(false);
  const [editingClient, setEditingClient] = useState<any>(null);
  
  const [newClientData, setNewClientData] = useState({
    name: "",
    adminName: "",
    email: "",
    password: "",
    subscriptionType: "trial",
    trialDays: 30,
    salePrice: "2000.00",
    notes: ""
  });

  // Cargar datos del revendedor desde localStorage
  useEffect(() => {
    const userData = localStorage.getItem("user");
    const authToken = localStorage.getItem("authToken");
    
    if (!userData || !authToken) {
      setLocation("/reseller-login");
      return;
    }
    
    try {
      const parsedUser = JSON.parse(userData);
      if (parsedUser.role !== 'reseller') {
        setLocation("/reseller-login");
        return;
      }
      setUser(parsedUser);
    } catch (error) {
      console.error("Error parsing user data:", error);
      setLocation("/reseller-login");
    }
  }, [setLocation]);

  // Obtener clientes creados por este revendedor
  const { data: clients, isLoading: clientsLoading } = useQuery<Client[]>({
    queryKey: ["/api/reseller-clients", user?.id],
    queryFn: async () => {
      const response = await fetch(`/api/reseller-clients?resellerId=${user?.id}&t=${Date.now()}`, {
        headers: { 
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`,
          "Cache-Control": "no-cache"
        },
      });
      if (!response.ok) throw new Error("Failed to fetch clients");
      return response.json();
    },
    enabled: !!user?.id,
  });

  // Obtener ventas del revendedor
  const { data: sales, isLoading: salesLoading } = useQuery<ResellerSale[]>({
    queryKey: ["/api/reseller-sales", user?.id],
    queryFn: async () => {
      const response = await fetch(`/api/reseller-sales?resellerId=${user?.id}&t=${Date.now()}`, {
        headers: { 
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`,
          "Cache-Control": "no-cache"
        },
      });
      if (!response.ok) throw new Error("Failed to fetch sales");
      return response.json();
    },
    enabled: !!user?.id,
  });

  // Actualizar cliente existente
  const updateClientMutation = useMutation({
    mutationFn: async ({ clientId, ...updateData }: any) => {
      const response = await fetch(`/api/reseller/client/${clientId}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`
        },
        body: JSON.stringify(updateData)
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error al actualizar cliente");
      }
      
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Cliente actualizado",
        description: "El cliente ha sido actualizado exitosamente.",
      });
      setEditingClient(null);
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-clients"] });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-sales"] });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al actualizar cliente",
        variant: "destructive",
      });
    },
  });

  // Eliminar cliente
  const deleteClientMutation = useMutation({
    mutationFn: async (clientId: number) => {
      const response = await fetch(`/api/reseller/client/${clientId}`, {
        method: "DELETE",
        headers: {
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`
        }
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error al eliminar cliente");
      }
      
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Cliente eliminado",
        description: "El cliente ha sido eliminado exitosamente.",
      });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-clients"] });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-sales"] });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al eliminar cliente",
        variant: "destructive",
      });
    },
  });

  // Crear nuevo cliente
  const createClientMutation = useMutation({
    mutationFn: async (clientData: any) => {
      const response = await fetch("/api/reseller/create-admin", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`
        },
        body: JSON.stringify({
          clientName: clientData.name,
          adminName: clientData.adminName,
          adminEmail: clientData.email,
          adminPassword: clientData.password,
          subscriptionType: clientData.subscriptionType || "premium",
          trialDays: clientData.trialDays || 30,
          salePrice: clientData.salePrice
        })
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error al crear cliente");
      }
      
      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: "Cliente creado",
        description: `Cliente y admin creados exitosamente. Cuota restante: ${data.quota?.remaining || 0}`,
      });
      setIsCreateClientDialogOpen(false);
      setNewClientData({
        name: "",
        adminName: "",
        email: "",
        password: "",
        subscriptionType: "trial",
        trialDays: 30,
        salePrice: "2000.00",
        notes: ""
      });
      // Refrescar datos del usuario para mostrar nueva cuota
      if (data.quota && user) {
        setUser({
          ...user,
          accountsSold: data.quota.sold
        });
      }
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-clients"] });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-sales"] });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al crear el cliente",
        variant: "destructive",
      });
    },
  });

  const handleLogout = () => {
    localStorage.removeItem("user");
    localStorage.removeItem("authToken");
    // Forzar navegación y recarga completa para limpiar estado
    window.location.href = '/reseller-login';
  };

  const formatCurrency = (amount: string) => {
    return new Intl.NumberFormat('es-AR', {
      style: 'currency',
      currency: 'ARS',
      minimumFractionDigits: 2
    }).format(parseFloat(amount));
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('es-AR');
  };

  if (!user) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-gray-400">Cargando panel...</p>
        </div>
      </div>
    );
  }

  const totalSalesAmount = sales?.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0) || 0;
  const totalProfit = sales?.reduce((sum, sale) => sum + parseFloat(sale.profit), 0) || 0;
  const soldCount = sales?.length || 0;
  const remainingQuota = (user.accountsQuota || 10) - soldCount;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Header */}
      <div className="bg-white dark:bg-gray-800 shadow-sm border-b border-gray-200 dark:border-gray-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                Panel de Revendedor
              </h1>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Bienvenido, {user.name}
              </p>
            </div>
            <div className="flex items-center space-x-4">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setLocation("/reseller-settings")}
              >
                <Settings className="w-4 h-4 mr-2" />
                Configuración
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={handleLogout}
              >
                <LogOut className="w-4 h-4 mr-2" />
                Cerrar Sesión
              </Button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Cuota Total</CardTitle>
              <Package className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{user.accountsQuota}</div>
              <p className="text-xs text-muted-foreground">
                Cuentas asignadas
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Vendidas</CardTitle>
              <Users className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{soldCount}</div>
              <p className="text-xs text-muted-foreground">
                Cuentas vendidas
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Disponibles</CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{remainingQuota}</div>
              <p className="text-xs text-muted-foreground">
                Cuentas restantes
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Ganancias</CardTitle>
              <DollarSign className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{formatCurrency(totalProfit.toString())}</div>
              <p className="text-xs text-muted-foreground">
                Total ganado
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Actions */}
        <div className="mb-6">
          <Dialog open={isCreateClientDialogOpen} onOpenChange={setIsCreateClientDialogOpen}>
            <DialogTrigger asChild>
              <Button className="bg-blue-600 hover:bg-blue-700">
                <Plus className="w-4 h-4 mr-2" />
                Crear Nuevo Cliente
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[425px]">
              <DialogHeader>
                <DialogTitle>Crear Nuevo Cliente</DialogTitle>
              </DialogHeader>
              <div className="grid gap-4 py-4">
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="name" className="text-right">Nombre Empresa</Label>
                  <Input
                    id="name"
                    value={newClientData.name}
                    onChange={(e) => setNewClientData({ ...newClientData, name: e.target.value })}
                    className="col-span-3"
                    placeholder="Nombre de la empresa"
                  />
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="adminName" className="text-right">Nombre Admin</Label>
                  <Input
                    id="adminName"
                    value={newClientData.adminName}
                    onChange={(e) => setNewClientData({ ...newClientData, adminName: e.target.value })}
                    className="col-span-3"
                    placeholder="Nombre del administrador"
                  />
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="email" className="text-right">Email</Label>
                  <Input
                    id="email"
                    type="email"
                    value={newClientData.email}
                    onChange={(e) => setNewClientData({ ...newClientData, email: e.target.value })}
                    className="col-span-3"
                  />
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="password" className="text-right">Contraseña *</Label>
                  <Input
                    id="password"
                    type="password"
                    value={newClientData.password}
                    onChange={(e) => setNewClientData({ ...newClientData, password: e.target.value })}
                    className="col-span-3"
                    placeholder="Contraseña para el admin"
                  />
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="subscriptionType" className="text-right">Suscripción</Label>
                  <select
                    id="subscriptionType"
                    value={newClientData.subscriptionType}
                    onChange={(e) => setNewClientData({ ...newClientData, subscriptionType: e.target.value })}
                    className="col-span-3 flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <option value="trial">Prueba Gratuita</option>
                    <option value="premium">Premium</option>
                    <option value="unlimited">Ilimitado</option>
                  </select>
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="trialDays" className="text-right">Días de Prueba</Label>
                  <Input
                    id="trialDays"
                    type="number"
                    value={newClientData.trialDays}
                    onChange={(e) => setNewClientData({ ...newClientData, trialDays: parseInt(e.target.value) })}
                    className="col-span-3"
                  />
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="salePrice" className="text-right">Precio de Venta</Label>
                  <Input
                    id="salePrice"
                    value={newClientData.salePrice}
                    onChange={(e) => setNewClientData({ ...newClientData, salePrice: e.target.value })}
                    className="col-span-3"
                  />
                </div>
                <div className="grid grid-cols-4 items-center gap-4">
                  <Label htmlFor="notes" className="text-right">Notas</Label>
                  <Textarea
                    id="notes"
                    value={newClientData.notes}
                    onChange={(e) => setNewClientData({ ...newClientData, notes: e.target.value })}
                    className="col-span-3"
                    rows={3}
                  />
                </div>
              </div>
              <div className="flex justify-end space-x-2">
                <Button variant="outline" onClick={() => setIsCreateClientDialogOpen(false)}>
                  Cancelar
                </Button>
                <Button 
                  onClick={() => {
                    if (!newClientData.name || !newClientData.adminName || !newClientData.email || !newClientData.password) {
                      toast({
                        title: "Campos requeridos",
                        description: "Por favor complete todos los campos obligatorios",
                        variant: "destructive"
                      });
                      return;
                    }
                    createClientMutation.mutate(newClientData);
                  }}
                  disabled={createClientMutation.isPending || remainingQuota <= 0}
                >
                  {createClientMutation.isPending ? "Creando..." : "Crear Cliente"}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
        </div>

        {/* Recent Sales */}
        <Card>
          <CardHeader>
            <CardTitle>Ventas Recientes</CardTitle>
          </CardHeader>
          <CardContent>
            {salesLoading ? (
              <div className="text-center py-4">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-2"></div>
                <p className="text-gray-600 dark:text-gray-400">Cargando ventas...</p>
              </div>
            ) : sales && sales.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Cliente</TableHead>
                    <TableHead>Email</TableHead>
                    <TableHead>Tipo</TableHead>
                    <TableHead>Precio</TableHead>
                    <TableHead>Ganancia</TableHead>
                    <TableHead>Fecha</TableHead>
                    <TableHead>Estado</TableHead>
                    <TableHead>Acciones</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sales.map((sale) => (
                    <TableRow key={sale.id}>
                      <TableCell className="font-medium">{sale.clientName}</TableCell>
                      <TableCell>{sale.clientEmail}</TableCell>
                      <TableCell>
                        <Badge variant={
                          sale.subscriptionType === 'trial' ? 'secondary' :
                          sale.subscriptionType === 'premium' ? 'default' : 'destructive'
                        }>
                          {sale.subscriptionType}
                        </Badge>
                      </TableCell>
                      <TableCell>{formatCurrency(sale.salePrice)}</TableCell>
                      <TableCell className="text-green-600">{formatCurrency(sale.profit)}</TableCell>
                      <TableCell>{formatDate(sale.saleDate)}</TableCell>
                      <TableCell>
                        <Badge variant="default">Activa</Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex space-x-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => {
                              // Find client data and set for editing
                              const clientData = clients?.find(c => c.id === sale.clientId);
                              if (clientData) {
                                setEditingClient({
                                  ...clientData,
                                  saleData: sale
                                });
                              }
                            }}
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => {
                              if (window.confirm(`¿Estás seguro de eliminar el cliente ${sale.clientName} y toda su información?`)) {
                                deleteClientMutation.mutate(sale.clientId);
                              }
                            }}
                          >
                            <Trash2 className="h-4 w-4 text-red-500" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="text-center py-8">
                <p className="text-gray-600 dark:text-gray-400">No hay ventas registradas aún.</p>
                <p className="text-sm text-gray-500 dark:text-gray-500 mt-2">
                  Crea tu primer cliente para comenzar a vender.
                </p>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Edit Client Dialog */}
        {editingClient && (
          <Dialog open={!!editingClient} onOpenChange={() => setEditingClient(null)}>
            <DialogContent className="sm:max-w-[600px]">
              <DialogHeader>
                <DialogTitle>Editar Cliente</DialogTitle>
              </DialogHeader>
              <div className="grid gap-4 py-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="edit-companyName">Nombre de Empresa</Label>
                    <Input
                      id="edit-companyName"
                      value={editingClient.name || ""}
                      onChange={(e) => setEditingClient(prev => ({ ...prev, name: e.target.value }))}
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-adminEmail">Email del Admin</Label>
                    <Input
                      id="edit-adminEmail"
                      type="email"
                      value={editingClient.adminEmail || ""}
                      onChange={(e) => setEditingClient(prev => ({ ...prev, adminEmail: e.target.value }))}
                      placeholder="admin@empresa.com"
                    />
                  </div>
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="edit-phone">Teléfono</Label>
                    <Input
                      id="edit-phone"
                      value={editingClient.phone || ""}
                      onChange={(e) => setEditingClient(prev => ({ ...prev, phone: e.target.value }))}
                    />
                  </div>
                  <div>
                    <Label htmlFor="edit-address">Dirección</Label>
                    <Input
                      id="edit-address"
                      value={editingClient.address || ""}
                      onChange={(e) => setEditingClient(prev => ({ ...prev, address: e.target.value }))}
                    />
                  </div>
                </div>

                <div>
                  <Label htmlFor="edit-subscriptionType">Tipo de Suscripción</Label>
                  <select
                    id="edit-subscriptionType"
                    value={editingClient.subscriptionType || "trial"}
                    onChange={(e) => setEditingClient(prev => ({ ...prev, subscriptionType: e.target.value }))}
                    className="w-full flex h-10 rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <option value="trial">Período de Prueba</option>
                    <option value="premium">Premium</option>
                    <option value="unlimited">Ilimitado</option>
                  </select>
                </div>
              </div>
              
              <div className="flex justify-end space-x-2">
                <Button variant="outline" onClick={() => setEditingClient(null)}>
                  Cancelar
                </Button>
                <Button 
                  onClick={() => {
                    updateClientMutation.mutate({
                      clientId: editingClient.id,
                      companyName: editingClient.name,
                      adminEmail: editingClient.adminEmail,
                      phone: editingClient.phone,
                      address: editingClient.address,
                      subscriptionType: editingClient.subscriptionType
                    });
                  }}
                  disabled={updateClientMutation.isPending}
                >
                  {updateClientMutation.isPending ? "Guardando..." : "Guardar Cambios"}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
        )}
      </div>
    </div>
  );
}