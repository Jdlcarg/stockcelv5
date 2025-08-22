# StockCel - Sistema de Gestión de Stock para Celulares

## Overview

StockCel is a comprehensive mobile phone inventory management system designed for retail businesses. The application provides multi-tenant support, allowing multiple companies to manage their phone inventory, sales, orders, payments, and cash operations within a single platform. The system supports different user roles (superuser, admin, vendor) with granular permissions and subscription-based access control.

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes

### Data Integrity and Historical Filtering System Enhancement (August 11, 2025)
- **COMPLETED**: Massive duplicate cleanup - removed 1,732 duplicate daily closure records from database
- **ENHANCED**: Daily reports filtering now limited to client creation date onwards to prevent historical data before account existence
- **IMPLEMENTED**: Working filter buttons (Week, Month, Year) for daily reports in cash-advanced interface
- **IMPROVED**: Backend daily reports endpoint now supports date range filtering based on client creation date
- **PROTECTED**: Historical data integrity by preventing display of reports from before client account creation
- **FIXED**: Duplicate endpoint issues in server routes causing conflicts
- **AUTOMATED**: System now validates all historical data against account creation timestamps

### Automatic Daily Closure System Implementation (August 11, 2025)
- **IMPLEMENTED**: Automatic daily closure system in auto-sync-monitor.ts to prevent data loss
- **ENHANCED**: Auto-sync-monitor now automatically performs daily closure at 23:59 if not done manually
- **SIMPLIFIED**: Removed "Forzar Cierre" and "Verificar ahora" buttons from cash-advanced.tsx interface
- **ADDED**: getCashRegisterByDate method to storage.ts for date-specific cash register retrieval
- **ADDED**: Test endpoint /api/cash-register/test-auto-close for verifying automatic closure functionality
- **AUTOMATED**: System now preserves complete cash movement history by ensuring daily closures never skip
- **PROTECTED**: Historical data loss prevention through automatic daily report generation
- **TIMEZONE**: Automatic closure uses Argentina timezone (America/Argentina/Buenos_Aires)

### VPS Migration Issues Resolution (August 8, 2025)
- **IDENTIFIED**: Email recovery system requires GMAIL_USER and GMAIL_PASS environment variables for VPS deployment
- **IDENTIFIED**: User deletion functionality requires PostgreSQL CASCADE DELETE constraints for proper operation
- **CREATED**: Complete VPS configuration guide (vps-configuration-fix.md) with step-by-step solutions
- **CREATED**: Automated installation script (fix-vps-issues.sh) for environment setup
- **CREATED**: Database constraints fix script (fix-database-constraints.sql) for proper CASCADE DELETE
- **CREATED**: Testing script (test-vps-fixes.js) to verify all configurations
- **DOCUMENTED**: Gmail App Password configuration process for production email sending
- **RESOLVED**: Payment method filtering bug in Cash Advanced (Cajas Avanzadas) section

### Cash Management System Enhancement - Complete Historical View (August 8, 2025)
- **IMPLEMENTED**: Removed daily reset behavior from cash movements display
- **ENHANCED**: Cash Advanced (Cajas Avanzadas) now shows complete historical movements by default
- **MODIFIED**: Backend real-time state calculation to process all historical movements instead of daily-filtered data
- **UPDATED**: API endpoints to return complete historical data without date filtering restrictions
- **IMPROVED**: Frontend with better date range filtering controls and quick action buttons for common date ranges
- **ADDED**: Quick filter buttons for "Today", "Last Week", "Last Month" for easy navigation
- **ENHANCED**: User experience with clear indication that complete history is being displayed
- **MAINTAINED**: Existing functionality for totals, graphs, and payment method breakdowns using historical data

### Enhanced Admin Panel Security - Double Confirmation Deletion (August 8, 2025)
- **IMPLEMENTED**: Double-confirmation system for client deletion in SuperUser panel
- **ADDED**: First warning dialog explaining irreversible consequences 
- **ADDED**: Client movement verification endpoint `/api/admin/clients/:id/movements`
- **ADDED**: Second confirmation dialog showing detailed account data before final deletion
- **ENHANCED**: Complete cascade deletion system respecting all foreign key constraints
- **IMPROVED**: Visual warnings with emojis and color-coded alerts for critical actions
- **SECURITY**: Only SuperUsers can access enhanced deletion functionality

### Password Recovery System - Complete Implementation (August 8, 2025)
- **IMPLEMENTED**: Complete password recovery system with email sending
- **ADDED**: "¿Olvidaste tu contraseña?" link in login page
- **CREATED**: forgot-password.tsx page with email input and success states
- **ENHANCED**: reset-password.tsx page with token validation and password reset
- **CONFIGURED**: Gmail SMTP with application-specific password (recoverysoftwarepar@gmail.com)
- **ADDED**: Password reset token database table and storage methods
- **WORKING**: Complete email sending with professional HTML templates
- **SECURITY**: Tokens expire in 1 hour and are single-use only

### Client Management Fixes (August 8, 2025)
- **FIXED**: SuperUser client editing error - duplicate email validation added
- **IMPROVED**: Client update validation prevents email conflicts
- **ENHANCED**: Better error messages for duplicate email scenarios

### Reseller System Fixes (August 8, 2025)
- **FIXED**: Added customer observations column with "ver más" button and modal display
- **FIXED**: Password hashing for admin accounts created by resellers - bcrypt now working correctly
- **FIXED**: Added separate admin name and company name fields in reseller account creation form
- **FIXED**: Real-time quota display using actual sales count instead of cached user data
- **FIXED**: API caching issues causing stale data - added cache busting parameters
- **FIXED**: Missing edit/delete action buttons in reseller sales table
- **FIXED**: Configuration saving for reseller price settings now working correctly
- **ADDED**: Comprehensive form validation for all required fields in reseller panel

### Critical Bug Fixes (August 6, 2025)
- **FIXED**: Double password hash causing login failures after admin password changes
- **FIXED**: Vendor commission synchronization with cash register system
- **FIXED**: Admin permissions for company configuration updates and logo uploads
- **FIXED**: TypeScript error in vendor management interface
- **CREATED**: stockcel_correcciones_agosto2025.zip deployment package with all fixes

### Database Migration (August 2025) - CLEAN PRODUCTION
- New clean PostgreSQL setup with database: stockcel_software
- User: stockcel_software, Password: Kc5bpdfkr  
- Removed all unnecessary files (.zip, .md, logs, packages)
- Updated all configuration files for new database
- Login system with comprehensive debug logging
- Production-ready configuration without external dependencies

### VPS Documentation (August 2025)
- Complete technical documentation for VPS installation with local PostgreSQL
- Automated installation script with error handling
- Quick commands reference for maintenance
- Production deployment manual with security best practices

## System Architecture

### Frontend Architecture
- **Framework**: React with TypeScript using Vite as the build tool
- **UI Library**: shadcn/ui components built on Radix UI primitives with Tailwind CSS for styling
- **State Management**: TanStack Query (React Query) for server state management and caching
- **Routing**: Wouter for lightweight client-side routing
- **Forms**: React Hook Form with Zod validation for type-safe form handling
- **Authentication**: Custom hook-based authentication system with localStorage persistence

### Backend Architecture
- **Runtime**: Node.js with Express.js framework
- **Database ORM**: Drizzle ORM with PostgreSQL dialect
- **Database**: PostgreSQL (supports both local and hosted instances like Neon)
- **Authentication**: Simple header-based authentication with bcrypt for password hashing
- **File Structure**: Modular approach with separate storage layers for different domains
- **Automation**: Auto-sync-monitor system for automatic daily closures and data integrity

### Multi-Tenancy Design
- **Client-based isolation**: All data is scoped by `clientId` to ensure tenant separation
- **Subscription management**: Built-in subscription system with trial, premium, and unlimited tiers
- **Role-based access control**: Three-tier user system (superuser, admin, vendor) with granular permissions

### Core Domain Models
- **Inventory Management**: Products with IMEI tracking, status management, and quality control
- **Sales Operations**: Orders, order items, payments with multi-currency support (USD, ARS, USDT)
- **Cash Management**: Daily cash register operations, movements tracking, expenses, and automatic daily closures
- **User Management**: Multi-role user system with permission-based access
- **Customer Relations**: Customer database with purchase history and debt tracking
- **Automation**: Automatic daily closure system preventing historical data loss

### Data Storage Strategy
- **Primary Storage**: PostgreSQL database with connection pooling via postgres-js
- **Schema Management**: Drizzle migrations for version-controlled database changes
- **Connection Handling**: Environment-based database URL configuration supporting local and remote instances

### Authentication & Authorization
- **Password Security**: bcrypt hashing with mandatory password changes on first login
- **Session Management**: Header-based authentication with user ID transmission
- **Role Enforcement**: Route-level protection with role and permission checking
- **Password Recovery**: Email-based password reset with secure token generation

### Subscription System
- **Trial Management**: Time-based trial periods with automatic expiration
- **Premium Tiers**: Monthly and yearly subscription options
- **Access Control**: Feature blocking for expired subscriptions with exemptions for superusers
- **Billing Integration**: Contact-based sales process with manual subscription management

## External Dependencies

### Database Services
- **PostgreSQL**: Primary database supporting both local installations and cloud providers
- **Neon Database**: Optional cloud PostgreSQL provider (legacy support)
- **Connection**: postgres-js driver for high-performance database operations

### Email Services
- **Nodemailer**: SMTP email delivery for password resets and notifications
- **Gmail SMTP**: Primary email provider configuration for system communications

### Development Tools
- **Drizzle Kit**: Database schema management and migration tools
- **ESBuild**: Fast JavaScript bundling for production builds
- **PM2**: Process management for production deployments
- **TypeScript**: Full-stack type safety with shared schema definitions

### UI Component Libraries
- **Radix UI**: Comprehensive set of accessible UI primitives
- **Tailwind CSS**: Utility-first CSS framework with custom design system
- **Lucide React**: Icon library for consistent iconography throughout the application

### Data Management
- **TanStack Query**: Server state management with intelligent caching and background updates
- **Zod**: Runtime type validation for forms and API data
- **React Hook Form**: Performant form handling with minimal re-renders

### Authentication Libraries
- **bcryptjs**: Password hashing and verification
- **crypto**: Secure token generation for password resets
- **Custom auth system**: Header-based authentication with role management