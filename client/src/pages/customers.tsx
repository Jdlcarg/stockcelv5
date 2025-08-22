import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Pencil, Trash2, Plus, Users, Eye, FileText } from "lucide-react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { apiRequest } from "@/lib/queryClient";
import { toast } from "@/hooks/use-toast";
import type { Customer } from "@shared/schema";

const customerSchema = z.object({
  name: z.string().min(1, "Nombre es requerido"),
  phone: z.string().optional(),
  email: z.union([z.string().email("Email inválido"), z.literal("")]).optional(),
  address: z.string().optional(),
  identification: z.string().optional(),
  notes: z.string().optional(),
  isActive: z.boolean().default(true),
});

export default function CustomersPage() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const queryClient = useQueryClient();
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [editingCustomer, setEditingCustomer] = useState<Customer | null>(null);
  const [viewingNotes, setViewingNotes] = useState<Customer | null>(null);

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  const { data: customers, isLoading } = useQuery({
    queryKey: ["/api/customers", user?.clientId],
    queryFn: async () => {
      const response = await apiRequest("GET", `/api/customers?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const createCustomerMutation = useMutation({
    mutationFn: async (customerData: any) => {
      const payload = { ...customerData, clientId: user?.clientId };
      console.log("Creating customer with data:", payload);
      const response = await apiRequest("POST", "/api/customers", payload);
      return response.json();
    },
    onSuccess: (data) => {
      console.log("Customer created successfully:", data);
      queryClient.invalidateQueries({ queryKey: ["/api/customers", user?.clientId] });
      setIsCreateDialogOpen(false);
      toast({ title: "Cliente creado exitosamente" });
    },
    onError: (error) => {
      console.error("Error creating customer:", error);
      toast({ title: "Error al crear cliente", variant: "destructive" });
    },
  });

  const updateCustomerMutation = useMutation({
    mutationFn: async ({ id, ...customerData }: any) => {
      const response = await apiRequest("PUT", `/api/customers/${id}`, customerData);
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/customers", user?.clientId] });
      setEditingCustomer(null);
      toast({ title: "Cliente actualizado exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al actualizar cliente", variant: "destructive" });
    },
  });

  const deleteCustomerMutation = useMutation({
    mutationFn: async (id: number) => {
      const response = await apiRequest("DELETE", `/api/customers/${id}`);
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/customers", user?.clientId] });
      toast({ title: "Cliente eliminado exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al eliminar cliente", variant: "destructive" });
    },
  });

  const CustomerForm = ({ customer, onClose }: { customer?: Customer; onClose: () => void }) => {
    const form = useForm({
      resolver: zodResolver(customerSchema),
      defaultValues: {
        name: customer?.name || "",
        phone: customer?.phone || "",
        email: customer?.email || "",
        address: customer?.address || "",
        identification: customer?.identification || "",
        notes: customer?.notes || "",
        isActive: customer?.isActive ?? true,
      },
    });

    const onSubmit = (data: any) => {
      console.log("Form submitted with data:", data);
      console.log("Form errors:", form.formState.errors);
      console.log("Form is valid:", form.formState.isValid);
      
      if (customer) {
        updateCustomerMutation.mutate({ id: customer.id, ...data });
      } else {
        createCustomerMutation.mutate(data);
      }
    };

    return (
      <Form {...form}>
        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
          <FormField
            control={form.control}
            name="name"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Nombre *</FormLabel>
                <FormControl>
                  <Input placeholder="Nombre del cliente" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="phone"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Teléfono</FormLabel>
                <FormControl>
                  <Input placeholder="Teléfono del cliente" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="email"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Email</FormLabel>
                <FormControl>
                  <Input placeholder="Email del cliente" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="address"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Dirección</FormLabel>
                <FormControl>
                  <Input placeholder="Dirección del cliente" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="identification"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Identificación</FormLabel>
                <FormControl>
                  <Input placeholder="DNI, CI, etc." {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="notes"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Notas</FormLabel>
                <FormControl>
                  <Textarea placeholder="Notas adicionales del cliente" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="isActive"
            render={({ field }) => (
              <FormItem className="flex items-center space-x-2 space-y-0">
                <FormControl>
                  <Switch checked={field.value} onCheckedChange={field.onChange} />
                </FormControl>
                <FormLabel>Activo</FormLabel>
              </FormItem>
            )}
          />

          <div className="flex justify-end space-x-2">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancelar
            </Button>
            <Button type="submit" disabled={createCustomerMutation.isPending || updateCustomerMutation.isPending}>
              {customer ? "Actualizar" : "Crear"}
            </Button>
          </div>
        </form>
      </Form>
    );
  };

  if (!isAuthenticated) {
    return null;
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col">
        <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
        <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
        
        <div className="flex flex-1">
          <Sidebar />
          <main className="flex-1 bg-gray-50">
            <div className="flex items-center justify-center h-64">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          </main>
        </div>
        <Footer />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1 bg-gray-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="space-y-6">
              <div className="flex justify-between items-center">
                <div>
                  <h1 className="text-2xl font-bold">Clientes</h1>
                  <p className="text-gray-600">Gestiona los clientes de tu empresa</p>
                </div>
                <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
                  <DialogTrigger asChild>
                    <Button>
                      <Plus className="mr-2 h-4 w-4" />
                      Nuevo Cliente
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Crear Cliente</DialogTitle>
                    </DialogHeader>
                    <CustomerForm onClose={() => setIsCreateDialogOpen(false)} />
                  </DialogContent>
                </Dialog>
              </div>

              <Card>
                <CardHeader>
                  <CardTitle>Lista de Clientes</CardTitle>
                  <CardDescription>
                    Clientes registrados en el sistema
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Nombre</TableHead>
                        <TableHead>Teléfono</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Identificación</TableHead>
                        <TableHead>Observaciones</TableHead>
                        <TableHead>Estado</TableHead>
                        <TableHead>Acciones</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {customers?.map((customer: Customer) => (
                        <TableRow key={customer.id}>
                          <TableCell className="font-medium">
                            <div className="flex items-center gap-2">
                              <Users className="h-4 w-4" />
                              {customer.name}
                            </div>
                          </TableCell>
                          <TableCell>{customer.phone || "-"}</TableCell>
                          <TableCell>{customer.email || "-"}</TableCell>
                          <TableCell>{customer.identification || "-"}</TableCell>
                          <TableCell>
                            {customer.notes && customer.notes.trim() ? (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      className="h-8 p-2"
                                      onClick={() => setViewingNotes(customer)}
                                    >
                                      <FileText className="h-4 w-4 mr-1" />
                                      Ver más
                                    </Button>
                                  </TooltipTrigger>
                                  <TooltipContent className="max-w-xs">
                                    <p>{customer.notes.length > 50 ? `${customer.notes.substring(0, 50)}...` : customer.notes}</p>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            ) : (
                              "-"
                            )}
                          </TableCell>
                          <TableCell>
                            <Badge variant={customer.isActive ? "default" : "secondary"}>
                              {customer.isActive ? "Activo" : "Inactivo"}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <div className="flex space-x-2">
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => setEditingCustomer(customer)}
                              >
                                <Pencil className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => deleteCustomerMutation.mutate(customer.id)}
                              >
                                <Trash2 className="h-4 w-4 text-red-500" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>

              <Dialog open={!!editingCustomer} onOpenChange={() => setEditingCustomer(null)}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Editar Cliente</DialogTitle>
                  </DialogHeader>
                  {editingCustomer && (
                    <CustomerForm customer={editingCustomer} onClose={() => setEditingCustomer(null)} />
                  )}
                </DialogContent>
              </Dialog>

              <Dialog open={!!viewingNotes} onOpenChange={() => setViewingNotes(null)}>
                <DialogContent className="max-w-md">
                  <DialogHeader>
                    <DialogTitle className="flex items-center gap-2">
                      <FileText className="h-5 w-5" />
                      Observaciones - {viewingNotes?.name}
                    </DialogTitle>
                  </DialogHeader>
                  <div className="space-y-4">
                    <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
                      <p className="text-sm whitespace-pre-wrap">
                        {viewingNotes?.notes || "Sin observaciones"}
                      </p>
                    </div>
                    <div className="flex justify-end">
                      <Button onClick={() => setViewingNotes(null)}>
                        Cerrar
                      </Button>
                    </div>
                  </div>
                </DialogContent>
              </Dialog>
            </div>
          </div>
        </main>
      </div>
      <Footer />
    </div>
  );
}