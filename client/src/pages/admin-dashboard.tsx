import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";

import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { UserPlus, Building2, Calendar, Users, Edit, Trash2, Settings, Clock, CheckCircle, XCircle, AlertTriangle } from "lucide-react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useToast } from "@/hooks/use-toast";

// Edit Client Form Schema
const editClientSchema = z.object({
  name: z.string().min(1, "Nombre de empresa requerido"),
  email: z.string().email("Email inválido"),
  phone: z.string().optional(),
  address: z.string().optional(),
  isActive: z.boolean(),
  subscriptionType: z.enum(["trial", "premium_monthly", "premium_yearly", "unlimited", "expired"]),
  trialStartDate: z.string().optional(),
  trialEndDate: z.string().optional(),
  notes: z.string().optional(),
  salesContactNumber: z.string().optional(),
  newPassword: z.string().optional(),
});

type EditClientForm = z.infer<typeof editClientSchema>;

export default function AdminDashboard() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [editingClient, setEditingClient] = useState<any>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [secondConfirmDialogOpen, setSecondConfirmDialogOpen] = useState(false);
  const [clientToDelete, setClientToDelete] = useState<any>(null);
  const [clientMovements, setClientMovements] = useState<any>(null);
  const { isAuthenticated, user } = useAuth();
  const [, setLocation] = useLocation();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const { data: clients, isLoading } = useQuery({
    queryKey: ["/api/admin/clients"],
    enabled: isAuthenticated && user?.role === 'superuser',
  });

  const editForm = useForm<EditClientForm>({
    resolver: zodResolver(editClientSchema),
    defaultValues: {
      name: "",
      email: "",
      phone: "",
      address: "",
      isActive: true,
      subscriptionType: "trial",
      trialStartDate: "",
      trialEndDate: "",
      notes: "",
      salesContactNumber: "",
      newPassword: "",
    },
  });

  const updateClientMutation = useMutation({
    mutationFn: async (data: { id: number; updates: EditClientForm }) => {
      const authData = localStorage.getItem('auth');
      const userId = authData ? JSON.parse(authData).user?.id : null;
      
      const response = await fetch(`/api/admin/clients/${data.id}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": userId?.toString() || "",
        },
        body: JSON.stringify(data.updates),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error updating client");
      }

      return response.json();
    },
    onSuccess: (updatedClient) => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/clients"] });
      setEditingClient(null);
      editForm.reset();
      
      // Si el cliente editado está actualmente logueado, actualizar su info en localStorage
      const authData = localStorage.getItem('auth');
      if (authData) {
        const auth = JSON.parse(authData);
        if (auth.client && auth.client.id === updatedClient.id) {
          // Actualizar los datos del cliente en localStorage
          auth.client = {
            ...auth.client,
            subscriptionType: updatedClient.subscriptionType,
            trialStartDate: updatedClient.trialStartDate,
            trialEndDate: updatedClient.trialEndDate,
            salesContactNumber: updatedClient.salesContactNumber
          };
          localStorage.setItem('auth', JSON.stringify(auth));
          
          // Emitir evento personalizado para notificar la actualización
          window.dispatchEvent(new CustomEvent('clientUpdated', { 
            detail: { clientId: updatedClient.id, updatedData: auth.client } 
          }));
        }
      }
      
      toast({
        title: "Cliente actualizado",
        description: "Los datos del cliente han sido actualizados correctamente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al actualizar cliente",
        variant: "destructive",
      });
    },
  });

  // Consultar movimientos del cliente
  const checkClientMovementsMutation = useMutation({
    mutationFn: async (clientId: number) => {
      const authData = localStorage.getItem('auth');
      const userId = authData ? JSON.parse(authData).user?.id : null;
      
      const response = await fetch(`/api/admin/clients/${clientId}/movements`, {
        headers: {
          "x-user-id": userId?.toString() || "",
        },
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error checking client movements");
      }

      return response.json();
    },
    onSuccess: (movementsData) => {
      setClientMovements(movementsData);
      setDeleteDialogOpen(false);
      setSecondConfirmDialogOpen(true);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al verificar movimientos del cliente",
        variant: "destructive",
      });
    },
  });

  const deleteClientMutation = useMutation({
    mutationFn: async (clientId: number) => {
      const authData = localStorage.getItem('auth');
      const userId = authData ? JSON.parse(authData).user?.id : null;
      
      const response = await fetch(`/api/admin/clients/${clientId}`, {
        method: "DELETE",
        headers: {
          "x-user-id": userId?.toString() || "",
        },
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error deleting client");
      }

      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/clients"] });
      setDeleteDialogOpen(false);
      setSecondConfirmDialogOpen(false);
      setClientToDelete(null);
      setClientMovements(null);
      toast({
        title: "Cliente eliminado",
        description: "El cliente y todos sus datos han sido eliminados permanentemente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al eliminar cliente",
        variant: "destructive",
      });
      setDeleteDialogOpen(false);
      setSecondConfirmDialogOpen(false);
    },
  });

  const totalAdmins = clients?.filter((client: any) => client.id !== 13)?.length || 0;
  const activeAdmins = clients?.filter((client: any) => client.isActive && client.id !== 13)?.length || 0;
  const trialAdmins = clients?.filter((client: any) => client.subscriptionType === 'trial' && client.id !== 13)?.length || 0;
  const expiredAdmins = clients?.filter((client: any) => {
    if (client.subscriptionType !== 'trial' || !client.trialEndDate || client.id === 13) return false;
    return new Date() > new Date(client.trialEndDate);
  })?.length || 0;

  const handleEditClient = (client: any) => {
    setEditingClient(client);
    editForm.reset({
      name: client.name,
      email: client.adminEmail || client.email,
      phone: client.phone || "",
      address: client.address || "",
      isActive: client.isActive,
      subscriptionType: client.subscriptionType || "trial",
      trialStartDate: client.trialStartDate ? new Date(client.trialStartDate).toISOString().split('T')[0] : "",
      trialEndDate: client.trialEndDate ? new Date(client.trialEndDate).toISOString().split('T')[0] : "",
      notes: client.notes || "",
      salesContactNumber: client.salesContactNumber || "",
      newPassword: "", // Always empty for security
    });
  };

  const handleDeleteClient = (client: any) => {
    setClientToDelete(client);
    setDeleteDialogOpen(true);
  };

  const handleFirstConfirm = () => {
    if (clientToDelete) {
      checkClientMovementsMutation.mutate(clientToDelete.id);
    }
  };

  const handleFinalDelete = () => {
    if (clientToDelete) {
      deleteClientMutation.mutate(clientToDelete.id);
    }
  };

  const onEditSubmit = (data: EditClientForm) => {
    updateClientMutation.mutate({
      id: editingClient.id,
      updates: data,
    });
  };

  const getSubscriptionBadge = (client: any) => {
    const now = new Date();
    const endDate = client.trialEndDate ? new Date(client.trialEndDate) : null;
    
    if (client.subscriptionType === 'unlimited') {
      return <Badge className="bg-green-100 text-green-800"><CheckCircle className="w-3 h-3 mr-1" />Ilimitado</Badge>;
    } else if (client.subscriptionType === 'premium_yearly') {
      return <Badge className="bg-yellow-100 text-yellow-800"><CheckCircle className="w-3 h-3 mr-1" />Premium Anual</Badge>;
    } else if (client.subscriptionType === 'premium_monthly') {
      return <Badge className="bg-blue-100 text-blue-800"><CheckCircle className="w-3 h-3 mr-1" />Premium Mensual</Badge>;
    } else if (client.subscriptionType === 'expired') {
      return <Badge className="bg-red-100 text-red-800"><XCircle className="w-3 h-3 mr-1" />Expirado</Badge>;
    } else if (client.subscriptionType === 'trial') {
      if (endDate && now > endDate) {
        return <Badge className="bg-red-100 text-red-800"><XCircle className="w-3 h-3 mr-1" />Expirado</Badge>;
      } else {
        return <Badge className="bg-orange-100 text-orange-800"><Clock className="w-3 h-3 mr-1" />Prueba</Badge>;
      }
    } else {
      return <Badge className="bg-gray-100 text-gray-800"><Clock className="w-3 h-3 mr-1" />Desconocido</Badge>;
    }
  };

  const getDaysRemaining = (client: any) => {
    if (client.subscriptionType !== 'trial' || !client.trialEndDate) return null;
    
    const now = new Date();
    const endDate = new Date(client.trialEndDate);
    const diffTime = endDate.getTime() - now.getTime();
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    
    return diffDays > 0 ? diffDays : 0;
  };

  const setTrialPeriod = (days: number) => {
    const startDate = new Date();
    const endDate = new Date();
    endDate.setDate(endDate.getDate() + days);
    
    editForm.setValue('trialStartDate', startDate.toISOString().split('T')[0]);
    editForm.setValue('trialEndDate', endDate.toISOString().split('T')[0]);
  };

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    } else if (user?.role !== 'superuser') {
      setLocation("/dashboard");
    }
  }, [isAuthenticated, user, setLocation]);

  if (!isAuthenticated || user?.role !== 'superuser') {
    return null;
  }

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header 
        onMobileNavToggle={() => setMobileNavOpen(!mobileNavOpen)}
        isMobileNavOpen={mobileNavOpen}
        showMenuButton={true}
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
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <h1 className="text-3xl font-bold tracking-tight">Panel SuperUser</h1>
                <div className="flex gap-2">
                  <Button 
                    variant="outline" 
                    onClick={() => setLocation("/admin/system-configuration")}
                    className="flex items-center gap-2"
                  >
                    <Settings className="h-4 w-4" />
                    Configurar Sistema
                  </Button>
                  <Button asChild>
                    <a href="/admin/create">
                      <UserPlus className="h-4 w-4 mr-2" />
                      Crear Nuevo Admin
                    </a>
                  </Button>
                </div>
              </div>

              {/* Statistics Cards */}
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Admins</CardTitle>
                    <Building2 className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{totalAdmins}</div>
                    <p className="text-xs text-muted-foreground">
                      Empresas registradas
                    </p>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Activos</CardTitle>
                    <CheckCircle className="h-4 w-4 text-green-600" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold text-green-600">{activeAdmins}</div>
                    <p className="text-xs text-muted-foreground">
                      Admins con acceso
                    </p>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">En Prueba</CardTitle>
                    <Clock className="h-4 w-4 text-blue-600" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold text-blue-600">{trialAdmins}</div>
                    <p className="text-xs text-muted-foreground">
                      Período de prueba
                    </p>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Expirados</CardTitle>
                    <XCircle className="h-4 w-4 text-red-600" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold text-red-600">{expiredAdmins}</div>
                    <p className="text-xs text-muted-foreground">
                      Requieren renovación
                    </p>
                  </CardContent>
                </Card>
              </div>

              {/* Admins Table */}
              <Card>
                <CardHeader>
                  <CardTitle>Administración de Clientes</CardTitle>
                </CardHeader>
                <CardContent>
                  {isLoading ? (
                    <div className="text-center py-4">Cargando...</div>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Empresa</TableHead>
                          <TableHead>Admin</TableHead>
                          <TableHead>Email</TableHead>
                          <TableHead>Estado</TableHead>
                          <TableHead>Suscripción</TableHead>
                          <TableHead>Días Restantes</TableHead>
                          <TableHead>Fecha Creación</TableHead>
                          <TableHead>Acciones</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {clients?.filter((client: any) => client.id !== 13).map((client: any) => {
                          const daysRemaining = getDaysRemaining(client);
                          return (
                            <TableRow key={client.id}>
                              <TableCell className="font-medium">{client.name}</TableCell>
                              <TableCell>{client.adminUsername}</TableCell>
                              <TableCell>{client.adminEmail}</TableCell>
                              <TableCell>
                                {client.isActive ? (
                                  <Badge className="bg-green-100 text-green-800">Activo</Badge>
                                ) : (
                                  <Badge className="bg-red-100 text-red-800">Inactivo</Badge>
                                )}
                              </TableCell>
                              <TableCell>{getSubscriptionBadge(client)}</TableCell>
                              <TableCell>
                                {daysRemaining !== null ? (
                                  <span className={daysRemaining <= 7 ? "text-red-600 font-semibold" : "text-gray-600"}>
                                    {daysRemaining} días
                                  </span>
                                ) : (
                                  "—"
                                )}
                              </TableCell>
                              <TableCell>
                                {new Date(client.createdAt).toLocaleDateString()}
                              </TableCell>
                              <TableCell>
                                <div className="flex space-x-2">
                                  <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => handleEditClient(client)}
                                  >
                                    <Edit className="h-4 w-4" />
                                  </Button>
                                  <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => handleDeleteClient(client)}
                                    className="text-red-600 hover:text-red-700"
                                  >
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                </div>
                              </TableCell>
                            </TableRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  )}
                </CardContent>
              </Card>
            </div>
          </div>
        </main>
      </div>

      {/* Edit Client Dialog */}
      <Dialog open={!!editingClient} onOpenChange={() => setEditingClient(null)}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Editar Cliente</DialogTitle>
          </DialogHeader>
          <Form {...editForm}>
            <form onSubmit={editForm.handleSubmit(onEditSubmit)} className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <FormField
                  control={editForm.control}
                  name="name"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Nombre de Empresa</FormLabel>
                      <FormControl>
                        <Input {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                
                <FormField
                  control={editForm.control}
                  name="email"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Email</FormLabel>
                      <FormControl>
                        <Input type="email" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <FormField
                  control={editForm.control}
                  name="phone"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Teléfono</FormLabel>
                      <FormControl>
                        <Input {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                
                <FormField
                  control={editForm.control}
                  name="address"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Dirección</FormLabel>
                      <FormControl>
                        <Input {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <FormField
                  control={editForm.control}
                  name="isActive"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Estado</FormLabel>
                      <Select onValueChange={(value) => field.onChange(value === 'true')} value={field.value?.toString()}>
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="true">Activo</SelectItem>
                          <SelectItem value="false">Inactivo</SelectItem>
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                
                <FormField
                  control={editForm.control}
                  name="subscriptionType"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Tipo de Suscripción</FormLabel>
                      <Select onValueChange={field.onChange} value={field.value}>
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="trial">Período de Prueba</SelectItem>
                          <SelectItem value="premium_monthly">Premium Mensual</SelectItem>
                          <SelectItem value="premium_yearly">Premium Anual</SelectItem>
                          <SelectItem value="unlimited">Acceso Ilimitado</SelectItem>
                          <SelectItem value="expired">Expirado</SelectItem>
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </div>

              <div className="space-y-4">
                <div className="flex space-x-2">
                  <Button type="button" variant="outline" size="sm" onClick={() => setTrialPeriod(7)}>
                    7 días
                  </Button>
                  <Button type="button" variant="outline" size="sm" onClick={() => setTrialPeriod(15)}>
                    15 días
                  </Button>
                  <Button type="button" variant="outline" size="sm" onClick={() => setTrialPeriod(30)}>
                    30 días
                  </Button>
                  <Button type="button" variant="outline" size="sm" onClick={() => setTrialPeriod(60)}>
                    60 días
                  </Button>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <FormField
                    control={editForm.control}
                    name="trialStartDate"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Fecha Inicio Prueba</FormLabel>
                        <FormControl>
                          <Input type="date" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={editForm.control}
                    name="trialEndDate"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Fecha Fin Prueba</FormLabel>
                        <FormControl>
                          <Input type="date" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
              </div>

              <FormField
                control={editForm.control}
                name="notes"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Notas</FormLabel>
                    <FormControl>
                      <Textarea {...field} rows={3} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={editForm.control}
                name="salesContactNumber"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Número de Contacto de Ventas</FormLabel>
                    <FormControl>
                      <Input {...field} placeholder="Ej: +54 9 11 1234-5678" />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={editForm.control}
                name="newPassword"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Nueva Contraseña (opcional)</FormLabel>
                    <FormControl>
                      <Input 
                        {...field} 
                        type="password" 
                        placeholder="Dejar vacío para mantener la contraseña actual"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <div className="flex justify-end space-x-2 pt-4">
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => setEditingClient(null)}
                >
                  Cancelar
                </Button>
                <Button
                  type="submit"
                  disabled={updateClientMutation.isPending}
                >
                  {updateClientMutation.isPending ? "Guardando..." : "Guardar Cambios"}
                </Button>
              </div>
            </form>
          </Form>
        </DialogContent>
      </Dialog>

      {/* Primera Confirmación de Eliminación */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="text-red-600 flex items-center gap-2">
              <Trash2 className="h-5 w-5" />
              ⚠️ PRIMERA ADVERTENCIA - Eliminar Cliente
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="font-medium text-red-800">
                ¿Estás seguro de que quieres eliminar el cliente <strong>{clientToDelete?.name}</strong>?
              </p>
              <p className="text-sm text-red-700 mt-2">
                Esta acción es <strong>IRREVERSIBLE</strong> y eliminará permanentemente:
              </p>
              <ul className="text-sm text-red-700 mt-2 ml-4 list-disc">
                <li>Todos los productos del inventario</li>
                <li>Todas las ventas y órdenes</li>
                <li>Todos los usuarios administradores y vendedores</li>
                <li>Todo el historial de movimientos</li>
                <li>Configuraciones y reportes</li>
              </ul>
            </div>
            <p className="text-sm text-gray-600">
              Si continúas, se verificará si la cuenta tiene movimientos activos antes de proceder.
            </p>
            <div className="flex justify-end space-x-2">
              <Button
                variant="outline"
                onClick={() => setDeleteDialogOpen(false)}
              >
                Cancelar
              </Button>
              <Button
                variant="destructive"
                onClick={handleFirstConfirm}
                disabled={checkClientMovementsMutation.isPending}
              >
                {checkClientMovementsMutation.isPending ? "Verificando..." : "Continuar"}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Segunda Confirmación con Detalles de Movimientos */}
      <Dialog open={secondConfirmDialogOpen} onOpenChange={setSecondConfirmDialogOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="text-red-600 flex items-center gap-2">
              <Trash2 className="h-5 w-5" />
              🚨 SEGUNDA ADVERTENCIA - Confirmación Final
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="bg-red-100 border-2 border-red-300 rounded-lg p-4">
              <p className="font-bold text-red-800 text-lg">
                ¡ÚLTIMA OPORTUNIDAD!
              </p>
              <p className="text-red-700 mt-1">
                Estás a punto de eliminar permanentemente el cliente <strong>{clientToDelete?.name}</strong> y TODOS sus datos.
              </p>
            </div>

            {clientMovements && (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                <h3 className="font-medium text-yellow-800 mb-2">Datos encontrados en la cuenta:</h3>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  {clientMovements.products > 0 && (
                    <div className="flex justify-between">
                      <span>Productos:</span>
                      <span className="font-medium">{clientMovements.products}</span>
                    </div>
                  )}
                  {clientMovements.orders > 0 && (
                    <div className="flex justify-between">
                      <span>Órdenes/Ventas:</span>
                      <span className="font-medium">{clientMovements.orders}</span>
                    </div>
                  )}
                  {clientMovements.users > 0 && (
                    <div className="flex justify-between">
                      <span>Usuarios:</span>
                      <span className="font-medium">{clientMovements.users}</span>
                    </div>
                  )}
                  {clientMovements.customers > 0 && (
                    <div className="flex justify-between">
                      <span>Clientes:</span>
                      <span className="font-medium">{clientMovements.customers}</span>
                    </div>
                  )}
                  {clientMovements.cashMovements > 0 && (
                    <div className="flex justify-between">
                      <span>Movimientos de Caja:</span>
                      <span className="font-medium">{clientMovements.cashMovements}</span>
                    </div>
                  )}
                </div>
              </div>
            )}

            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="text-red-800 font-medium mb-2">⚠️ Esta acción:</p>
              <ul className="text-sm text-red-700 space-y-1">
                <li>• Es completamente IRREVERSIBLE</li>
                <li>• Eliminará TODA la información listada arriba</li>
                <li>• No se puede deshacer una vez confirmada</li>
                <li>• Puede tardar varios minutos en completarse</li>
              </ul>
            </div>

            <div className="flex justify-end space-x-2 pt-4">
              <Button
                variant="outline"
                onClick={() => {
                  setSecondConfirmDialogOpen(false);
                  setClientMovements(null);
                }}
              >
                Cancelar
              </Button>
              <Button
                variant="destructive"
                onClick={handleFinalDelete}
                disabled={deleteClientMutation.isPending}
                className="bg-red-600 hover:bg-red-700"
              >
                {deleteClientMutation.isPending ? "Eliminando Permanentemente..." : "SÍ, ELIMINAR DEFINITIVAMENTE"}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Footer />
    </div>
  );
}