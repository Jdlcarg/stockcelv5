import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
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
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import { Loader2, Plus, Users, TrendingUp, DollarSign, Edit, Trash2 } from "lucide-react";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import { apiRequest } from "@/lib/queryClient";

interface Reseller {
  id: number;
  name: string;
  email: string;
  phone?: string;
  company?: string;
  password?: string;
  accountsQuota: number;
  accountsSold: number;
  costPerAccount: string;
  totalPaid: string;
  totalEarnings: string;
  isActive: boolean;
  createdAt: string;
  updatedAt?: string;
}

interface ResellerSale {
  id: number;
  resellerId: number;
  clientId: number;
  clientName?: string;
  clientEmail?: string;
  costPerAccount: string;
  salePrice: string;
  profit: string;
  subscriptionType: string;
  trialDays: number;
  saleDate: string;
  notes?: string;
  createdAt: string;
}

export default function ResellersManagement() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false);
  const [editingReseller, setEditingReseller] = useState<Reseller | null>(null);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const [formData, setFormData] = useState({
    name: "",
    email: "",
    phone: "",
    company: "",
    password: "",
    accountsQuota: 10,
    costPerAccount: "1000.00",
  });

  // Queries
  const { data: resellers, isLoading } = useQuery<Reseller[]>({
    queryKey: ["/api/resellers"],
    queryFn: async () => {
      const response = await fetch("/api/resellers", {
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
      if (!response.ok) throw new Error("Failed to fetch resellers");
      return response.json();
    },
    enabled: !!user?.id,
  });

  const { data: resellerSales } = useQuery<ResellerSale[]>({
    queryKey: ["/api/reseller-sales"],
    queryFn: async () => {
      const response = await fetch("/api/reseller-sales", {
        headers: { "x-user-id": user?.id?.toString() || "" },
      });
      if (!response.ok) throw new Error("Failed to fetch reseller sales");
      return response.json();
    },
    enabled: false, // Temporarily disabled
  });

  // Mutations
  const createResellerMutation = useMutation({
    mutationFn: async (data: any) => {
      const response = await fetch("/api/resellers", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": user?.id?.toString() || "",
        },
        body: JSON.stringify(data),
      });
      if (!response.ok) throw new Error("Failed to create reseller");
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/resellers"] });
      setIsCreateDialogOpen(false);
      setFormData({
        name: "",
        email: "",
        phone: "",
        company: "",
        password: "",
        accountsQuota: 10,
        costPerAccount: "1000.00",
      });
      toast({
        title: "Revendedor creado",
        description: "El revendedor ha sido creado exitosamente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al crear revendedor",
        variant: "destructive",
      });
    },
  });

  const updateResellerMutation = useMutation({
    mutationFn: async ({ id, data }: { id: number; data: any }) => {
      const response = await fetch(`/api/resellers/${id}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": user?.id?.toString() || "",
        },
        body: JSON.stringify(data),
      });
      if (!response.ok) throw new Error("Failed to update reseller");
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/resellers"] });
      setIsEditDialogOpen(false);
      setEditingReseller(null);
      toast({
        title: "Revendedor actualizado",
        description: "Los datos del revendedor han sido actualizados.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al actualizar revendedor",
        variant: "destructive",
      });
    },
  });

  const deleteResellerMutation = useMutation({
    mutationFn: async (id: number) => {
      const response = await fetch(`/api/resellers/${id}`, {
        method: "DELETE",
        headers: {
          "x-user-id": user?.id?.toString() || "",
        },
      });
      if (!response.ok) throw new Error("Failed to delete reseller");
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/resellers"] });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller-sales"] });
      toast({
        title: "Revendedor eliminado",
        description: "El revendedor ha sido eliminado correctamente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al eliminar revendedor",
        variant: "destructive",
      });
    },
  });

  const handleCreateReseller = () => {
    createResellerMutation.mutate(formData);
  };

  const handleUpdateReseller = () => {
    if (editingReseller) {
      updateResellerMutation.mutate({
        id: editingReseller.id,
        data: {
          name: editingReseller.name,
          email: editingReseller.email,
          company: editingReseller.company,
          // Solo incluir contraseña si el usuario especificó una nueva
          ...(editingReseller.password && editingReseller.password.trim() !== "" 
            ? { password: editingReseller.password }
            : {}),
          phone: editingReseller.phone,
          accountsQuota: editingReseller.accountsQuota,
          costPerAccount: editingReseller.costPerAccount,
          isActive: editingReseller.isActive,
        },
      });
    }
  };

  const handleDeleteReseller = (id: number) => {
    deleteResellerMutation.mutate(id);
  };

  const openEditDialog = (reseller: Reseller) => {
    // No incluir la contraseña hasheada en el estado de edición
    const { password, ...resellerWithoutPassword } = reseller;
    setEditingReseller({ ...resellerWithoutPassword, password: "" });
    setIsEditDialogOpen(true);
  };

  // Statistics
  const totalResellers = resellers?.length || 0;
  const activeResellers = resellers?.filter(r => r.isActive)?.length || 0;
  const totalSales = resellerSales?.length || 0;
  const totalRevenue = resellerSales?.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0) || 0;

  if (user?.role !== "superuser") {
    return (
      <div className="flex h-screen bg-gray-100 dark:bg-gray-900">
        <Sidebar />
        <div className="flex-1 flex flex-col overflow-hidden">
          <Header onMobileMenuToggle={() => setIsMobileMenuOpen(!isMobileMenuOpen)} />
          <main className="flex-1 overflow-x-hidden overflow-y-auto bg-gray-100 dark:bg-gray-900 p-6">
            <div className="text-center py-8">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-white">
                Acceso Denegado
              </h2>
              <p className="text-gray-600 dark:text-gray-400 mt-2">
                Solo los SuperUsuarios pueden acceder a la gestión de revendedores.
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
        <Header onMobileMenuToggle={() => setIsMobileMenuOpen(!isMobileMenuOpen)} />
        <main className="flex-1 overflow-x-hidden overflow-y-auto bg-gray-100 dark:bg-gray-900 p-6">
          <div className="max-w-7xl mx-auto">
            <div className="mb-6">
              <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
                Gestión de Revendedores
              </h1>
              <p className="text-gray-600 dark:text-gray-400 mt-2">
                Administra revendedores, asigna cuotas y monitorea ventas
              </p>
            </div>

            {/* Statistics Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">Total Revendedores</CardTitle>
                  <Users className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">{totalResellers}</div>
                  <p className="text-xs text-muted-foreground">
                    {activeResellers} activos
                  </p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">Ventas Totales</CardTitle>
                  <TrendingUp className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">{totalSales}</div>
                  <p className="text-xs text-muted-foreground">
                    Cuentas vendidas
                  </p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">Ingresos Totales</CardTitle>
                  <DollarSign className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">${totalRevenue.toFixed(2)}</div>
                  <p className="text-xs text-muted-foreground">
                    Facturación total
                  </p>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">Costo Promedio por Cuenta</CardTitle>
                  <DollarSign className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">$1,000</div>
                  <p className="text-xs text-muted-foreground">
                    Costo fijo por cuenta
                  </p>
                </CardContent>
              </Card>
            </div>

            {/* Actions */}
            <div className="mb-6">
              <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
                <DialogTrigger asChild>
                  <Button>
                    <Plus className="mr-2 h-4 w-4" />
                    Crear Revendedor
                  </Button>
                </DialogTrigger>
                <DialogContent className="sm:max-w-[500px]">
                  <DialogHeader>
                    <DialogTitle>Crear Nuevo Revendedor</DialogTitle>
                  </DialogHeader>
                  <div className="grid gap-4 py-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="name">Nombre Completo</Label>
                        <Input
                          id="name"
                          value={formData.name}
                          onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                          placeholder="Nombre del revendedor"
                        />
                      </div>
                      <div>
                        <Label htmlFor="email">Email</Label>
                        <Input
                          id="email"
                          type="email"
                          value={formData.email}
                          onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
                          placeholder="email@ejemplo.com"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="company">Empresa</Label>
                        <Input
                          id="company"
                          value={formData.company}
                          onChange={(e) => setFormData(prev => ({ ...prev, company: e.target.value }))}
                          placeholder="Nombre de la empresa"
                        />
                      </div>
                      <div>
                        <Label htmlFor="password">Contraseña</Label>
                        <Input
                          id="password"
                          type="password"
                          value={formData.password}
                          onChange={(e) => setFormData(prev => ({ ...prev, password: e.target.value }))}
                          placeholder="********"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="phone">Teléfono</Label>
                        <Input
                          id="phone"
                          value={formData.phone}
                          onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
                          placeholder="+54 11 1234-5678"
                        />
                      </div>
                      <div>
                        <Label htmlFor="accountsQuota">Cuota de Cuentas</Label>
                        <Input
                          id="accountsQuota"
                          type="number"
                          value={formData.accountsQuota}
                          onChange={(e) => setFormData(prev => ({ ...prev, accountsQuota: parseInt(e.target.value) }))}
                        />
                      </div>
                    </div>
                    <div>
                      <Label htmlFor="costPerAccount">Costo por Cuenta (ARS)</Label>
                      <Input
                        id="costPerAccount"
                        value={formData.costPerAccount}
                        onChange={(e) => setFormData(prev => ({ ...prev, costPerAccount: e.target.value }))}
                        placeholder="1000.00"
                      />
                    </div>
                  </div>
                  <div className="flex justify-end gap-2">
                    <Button variant="outline" onClick={() => setIsCreateDialogOpen(false)}>
                      Cancelar
                    </Button>
                    <Button 
                      onClick={handleCreateReseller}
                      disabled={createResellerMutation.isPending}
                    >
                      {createResellerMutation.isPending && (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      )}
                      Crear Revendedor
                    </Button>
                  </div>
                </DialogContent>
              </Dialog>
            </div>

            {/* Resellers Table */}
            <Card>
              <CardHeader>
                <CardTitle>Lista de Revendedores</CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="flex justify-center items-center py-8">
                    <Loader2 className="h-8 w-8 animate-spin" />
                  </div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Nombre</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Empresa</TableHead>
                        <TableHead>Cuota</TableHead>
                        <TableHead>Vendidas</TableHead>
                        <TableHead>Costo por Cuenta</TableHead>
                        <TableHead>Estado</TableHead>
                        <TableHead>Acciones</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {resellers?.map((reseller) => (
                        <TableRow key={reseller.id}>
                          <TableCell className="font-medium">{reseller.name}</TableCell>
                          <TableCell>{reseller.email}</TableCell>
                          <TableCell>{reseller.company || "-"}</TableCell>
                          <TableCell>{reseller.accountsQuota}</TableCell>
                          <TableCell>{reseller.accountsSold}</TableCell>
                          <TableCell>${reseller.costPerAccount}</TableCell>
                          <TableCell>
                            <Badge variant={reseller.isActive ? "default" : "secondary"}>
                              {reseller.isActive ? "Activo" : "Inactivo"}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <div className="flex gap-2">
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => openEditDialog(reseller)}
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
                                    <AlertDialogTitle>¿Eliminar revendedor?</AlertDialogTitle>
                                    <AlertDialogDescription>
                                      Esta acción no se puede deshacer. Se eliminarán también todas las ventas asociadas.
                                    </AlertDialogDescription>
                                  </AlertDialogHeader>
                                  <AlertDialogFooter>
                                    <AlertDialogCancel>Cancelar</AlertDialogCancel>
                                    <AlertDialogAction
                                      onClick={() => handleDeleteReseller(reseller.id)}
                                      className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                                    >
                                      Eliminar
                                    </AlertDialogAction>
                                  </AlertDialogFooter>
                                </AlertDialogContent>
                              </AlertDialog>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>

            {/* Edit Dialog */}
            <Dialog open={isEditDialogOpen} onOpenChange={setIsEditDialogOpen}>
              <DialogContent className="sm:max-w-[500px]">
                <DialogHeader>
                  <DialogTitle>Editar Revendedor</DialogTitle>
                </DialogHeader>
                {editingReseller && (
                  <div className="grid gap-4 py-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="edit-name">Nombre Completo</Label>
                        <Input
                          id="edit-name"
                          value={editingReseller.name}
                          onChange={(e) => setEditingReseller(prev => prev ? { ...prev, name: e.target.value } : null)}
                        />
                      </div>
                      <div>
                        <Label htmlFor="edit-email">Email</Label>
                        <Input
                          id="edit-email"
                          type="email"
                          value={editingReseller.email}
                          onChange={(e) => setEditingReseller(prev => prev ? { ...prev, email: e.target.value } : null)}
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="edit-username">Usuario</Label>
                        <Input
                          id="edit-username"
                          value={editingReseller.username}
                          onChange={(e) => setEditingReseller(prev => prev ? { ...prev, username: e.target.value } : null)}
                        />
                      </div>
                      <div>
                        <Label htmlFor="edit-password">Nueva Contraseña (opcional)</Label>
                        <Input
                          id="edit-password"
                          type="password"
                          value={editingReseller.password || ""}
                          onChange={(e) => setEditingReseller(prev => prev ? { ...prev, password: e.target.value } : null)}
                          placeholder="Dejar vacío para mantener actual"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label htmlFor="edit-phone">Teléfono</Label>
                        <Input
                          id="edit-phone"
                          value={editingReseller.phone || ""}
                          onChange={(e) => setEditingReseller(prev => prev ? { ...prev, phone: e.target.value } : null)}
                        />
                      </div>
                      <div>
                        <Label htmlFor="edit-accountsQuota">Cuota de Cuentas</Label>
                        <Input
                          id="edit-accountsQuota"
                          type="number"
                          value={editingReseller.accountsQuota}
                          onChange={(e) => setEditingReseller(prev => prev ? { ...prev, accountsQuota: parseInt(e.target.value) } : null)}
                        />
                      </div>
                    </div>
                    <div>
                      <Label htmlFor="edit-commission">Comisión (%)</Label>
                      <Input
                        id="edit-commission"
                        value={editingReseller.commission}
                        onChange={(e) => setEditingReseller(prev => prev ? { ...prev, commission: e.target.value } : null)}
                      />
                    </div>
                  </div>
                )}
                <div className="flex justify-end gap-2">
                  <Button variant="outline" onClick={() => setIsEditDialogOpen(false)}>
                    Cancelar
                  </Button>
                  <Button 
                    onClick={handleUpdateReseller}
                    disabled={updateResellerMutation.isPending}
                  >
                    {updateResellerMutation.isPending && (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    )}
                    Actualizar
                  </Button>
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </main>
      </div>
    </div>
  );
}